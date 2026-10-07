# Arquitetura

Este documento explica **como** as garantias do desafio são obtidas, **por que** cada escolha foi feita e **o que ficou de fora**. O ponto de partida foi um princípio: o banco é a última linha de defesa. Toda invariante financeira importante existe duas vezes, uma no domínio (para dar erro claro e cedo) e outra no schema (para que nem um bug, nem um operador, nem uma instância antiga consiga violá-la).

## 1. Visão geral

```
            HTTP (3 instâncias)            SQS wager-transactions.fifo
                   │                                 │
                   ▼                                 ▼
          WageringController               WagerConsumer (inbox)
                   └──────────────┬──────────────────┘
                                  ▼
                     ProcessWagerTransaction   ◄── mesmo use case nas duas entradas
                                  │  1 transação SQL
     ┌───────────────┬────────────┼──────────────┬───────────────┐
  inbox_messages  wager_transactions  wallets  wallet_ledger_entries  outbox_messages
                                                                        │
                                       OutboxPublisher (lease + SKIP LOCKED)
                                                                        ▼
                                                        SQS wagering-events.fifo

   PendingReferenceWorker ──► ResolvePendingReference (mesma ordem de locks)
```

As camadas seguem o fluxo `domain ← application ← infrastructure / interface`:

- **domain/**: classes com construtor privado e factories `create`/`rehydrate`, sem dependência de NestJS nem de ORM. As regras da seção 7 estão em uma função pura, [`settleWagerTransaction`](src/domain/wager-settlement.ts), que recebe a transação, a wallet (já travada) e a referência resolvida, e devolve `processed | rejected | pending_reference`.
- **application/**: use cases e **portas** (`UnitOfWork`, repositórios, `Clock`, `IdGenerator`, `WageringMetrics`). Os testes de unidade rodam os use cases com adaptadores em memória.
- **infrastructure/**: MikroORM, SQS, workers e observabilidade. Composição em [`app.module.ts`](src/app.module.ts).

## 2. Dinheiro

- `Money` guarda **centavos em `bigint`**: aritmética exata, sem `number` em nenhum ponto do caminho financeiro.
- **Entrada:** string decimal posicional com 0 a 2 casas (`"25"`, `"25.5"` e `"25.00"` são aceitos e normalizados para `"25.00"`). São rejeitados `NaN`, `Infinity`, notação científica, string vazia, espaços, vírgula, sinal `+`, mais de 2 casas e valores negativos. **Nada é arredondado**: `"1.005"` é erro, não `"1.01"`.
- **Saída:** sempre escala 2.
- **Persistência:** `NUMERIC(20,2)` em uma coluna e a moeda em `CHAR(3)` em outra. O driver `pg` devolve `NUMERIC` como string, e os mappers reconstroem o `Money` via `Money.from`. Não há tipo monetário do ORM. As somas da reconciliação são feitas pelo Postgres em `NUMERIC` e voltam como texto.
- **Moeda:** o modelo é multi-moeda (wallet única por `playerId + currency`, FK composta `(wallet_id, currency)` no ledger). Operar em moeda diferente da wallet gera `REJECTED / CURRENCY_MISMATCH`; misturar moedas dentro do domínio lança `CurrencyMismatchError`.

## 3. Persistência e schema

A migration [`Migration20261006000001_initial_schema`](src/infrastructure/persistence/migrations/Migration20261006000001_initial_schema.ts) é versionada e reversível. Os testes rodam `up → down → up`.

| Garantia | Como o banco garante |
|---|---|
| uma wallet por player + moeda | `UNIQUE (player_id, currency)` |
| saldo nunca negativo | `CHECK (balance >= 0)` na wallet e `CHECK` nos saldos do ledger |
| idempotência persistente | `UNIQUE (idempotency_key)` e `UNIQUE (provider_id, external_transaction_id)` |
| no máximo 1 lançamento por transação/wallet | `UNIQUE (transaction_id, wallet_id)` |
| lançamento aritmeticamente correto | `CHECK (balance_after = balance_before ± amount)` |
| ledger imutável | trigger que rejeita `UPDATE`, `DELETE` e `TRUNCATE` |
| lançamento na moeda da wallet | FK composta `(wallet_id, currency) → wallets(id, currency)` |
| saldo ⇔ ledger | *constraint trigger* `DEFERRABLE INITIALLY DEFERRED`: no **COMMIT**, o saldo da wallet precisa ser igual ao `balance_after` do último lançamento, e `version` igual ao `wallet_version` dele |
| ledger encadeado | trigger diferido: `balance_before` do lançamento N = `balance_after` do N-1 (ou 0 se não houver anterior) |
| ledger só para transação aplicada | trigger diferido: a transação existe, está `PROCESSED`, não é `LOSS`, é da mesma wallet e tem o mesmo valor |
| `version` só muda junto com o saldo, de 1 em 1 | trigger `BEFORE UPDATE` em `wallets` |
| estados terminais imutáveis | trigger `BEFORE UPDATE` em `wager_transactions`, que também congela todas as colunas de negócio |
| reversão única por referência | índice único parcial em `reference_transaction_id WHERE kind IN ('REFUND','ROLLBACK') AND status = 'PROCESSED'` |
| `OPENING` é interno | `CHECK ((kind = 'OPENING') = (provider_id = '__internal__'))` |
| inbox | `PRIMARY KEY (consumer_name, message_id)` |

O ledger tem uma linha por versão da wallet (`UNIQUE (wallet_id, wallet_version)`). Por isso `wallet_version` serve ao mesmo tempo de chave de encadeamento e de **cursor estável** da paginação (`/ledger?cursor=` é o base64url de `{ v: <última versão> }`).

### Por que MikroORM, e como ele é usado

O MikroORM é a opção preferida do enunciado. Ele fornece `em.transactional()` (fronteira de transação), pool de conexões, QueryBuilder tipado, `LockMode` e o Migrator, este com lista explícita de migrations (`migrationsList`), o que funciona igual a partir do código-fonte, de bundle ou de container.

A decisão consciente foi **não usar o dirty-checking do Unit of Work nos caminhos de escrita**. Os repositórios usam `QueryBuilder.execute()`, `em.insert` e `em.nativeUpdate`, e cada escrita é um único statement explícito, com semântica de lock/conflito visível no review:

- `SELECT … FOR UPDATE` via `LockMode.PESSIMISTIC_WRITE`;
- `INSERT … ON CONFLICT DO NOTHING` no inbox;
- `UPDATE wallets … WHERE id = ? AND version = ?` com checagem de `affectedRows`.

Ler com `execute()` evita o identity map: uma releitura **depois** de pegar o lock sempre vai ao banco, nunca devolve a entidade em cache de antes do lock. As entidades de persistência (`EntitySchema`, sem decorators) são separadas das classes de domínio, e mappers fazem a conversão usando `rehydrate`.

## 4. Concorrência

**Unidade de concorrência: a linha da wallet.**

1. `SELECT … FOR UPDATE` na wallet (lock pessimista **por wallet**, nunca global). Todas as alterações de uma wallet se serializam; wallets diferentes não se bloqueiam. O teste `3. distinct wallets` prova isso: mantém o lock da wallet A aberto e mostra que a wallet B liquida em menos de 1 s.
2. Depois do lock, a idempotência é **re-checada** (uma duplicata concorrente pode ter commitado enquanto esperávamos).
3. A atualização é condicional: `WHERE version = ?`. Com o lock isso nunca deveria falhar; se falhar (`ConcurrencyConflictError`), a transação toda é desfeita e reexecutada (no máximo 3 tentativas).
4. Corridas que só o banco vê — duas requisições com a mesma idempotency key em wallets **diferentes**, por exemplo — terminam em violação de `UNIQUE`. Isso vira `DuplicateRaceError`, e a nova tentativa enxerga a linha vencedora e responde replay ou conflito.

**Por que pessimista e não otimista?** A seção 8 descreve uma *hot wallet* disputada. Com lock otimista, sob disputa, a maioria das tentativas falha e é refeita, e o pior caso não é limitado. Com o lock da linha, a fila de espera é ordenada pelo próprio Postgres e cada transação dura poucos milissegundos. O `version` continua no modelo como defesa em profundidade e como versão pública da wallet.

**Isolation level:** `READ COMMITTED`. A correção não depende de `SERIALIZABLE`: vem do lock explícito, das constraints e dos triggers diferidos. Isso evita *serialization failures* espúrias sob carga.

**Backpressure:** `lock_timeout = 5s` por transação. Uma wallet quente demais devolve `503` (retentável), em vez de segurar conexões indefinidamente. O teste da hot wallet reenvia esses 503 como um provedor faria e mostra que cada operação existe exatamente uma vez.

**Métrica de contenção:** antes do `FOR UPDATE` bloqueante, o repositório faz um `FOR UPDATE SKIP LOCKED`. Se a linha existe mas veio vazia, outra transação segurava o lock, e isso conta em `wagering_lock_conflicts_total{type="contended"}`. A sonda não aborta a transação (diferente de `NOWAIT`), então custa só um round-trip, e só quando há disputa.

### Ordem de locks

Os dois caminhos de escrita — o use case principal e o resolvedor de referências pendentes — pegam **sempre o lock da wallet primeiro** e só depois leem ou atualizam a transação. Com uma ordem única de locks, os dois caminhos não entram em deadlock entre si.

## 5. Idempotência

- O header `Idempotency-Key` é obrigatório e é a fonte da verdade. O recomendado é `{providerId}:{externalTransactionId}`, mas qualquer valor ASCII imprimível de até 255 caracteres serve.
- **`payloadHash`** = SHA-256 (hex) do **JSON canônico** dos campos de negócio: `providerId, externalTransactionId, playerId, walletId, roundId, gameId, kind, money{amount normalizado, currency}, referenceExternalTransactionId`. JSON canônico aqui significa chaves ordenadas recursivamente, sem espaços e com campos `undefined` omitidos. Números são proibidos no payload hasheado, porque valores já chegam normalizados como string. O header e os metadados de transporte não entram no hash. Implementação: [`payload-hash.ts`](src/application/payload-hash.ts).
- **Mesma key + mesmo hash** → replay: mesmo status, mesmo corpo, mesmo saldo observado na época (`result_balance` é gravado na transação), `idempotentReplay: true`.
- **Mesma key + hash diferente** → `409 IDEMPOTENCY_CONFLICT`, nunca replay.
- **Mesmo `(providerId, externalTransactionId)` com outra key** → também `409`. Uma operação do provedor não pode existir duas vezes.
- Nada fica em memória: a deduplicação inteira está nas constraints `UNIQUE`.
- **Corrida encontrada pelos testes:** a busca do replay faz duas consultas (por key, depois por id externo). Em `READ COMMITTED`, uma duplicata concorrente pode commitar *entre* as duas e ser achada só pela segunda. A primeira versão respondia 409 nesse caso, para uma requisição idêntica. Agora, um registro com a **mesma** key é tratado como replay (com checagem de hash), e só uma key **diferente** é conflito. O teste "50× em paralelo" pegou o caso, e uma regressão determinística cobre a janela.

## 6. Regras de negócio e interpretações adotadas

Implementadas em [`wager-settlement.ts`](src/domain/wager-settlement.ts), na ordem: dono da wallet → moeda → referência (existe? está pendente? tipo permitido? mesmo escopo? `PROCESSED`? valor igual? já revertida?) → saldo.

Interpretações que o enunciado deixava em aberto:

1. **Reversão única por referência, independentemente do tipo.** O enunciado diz "não pode ser revertida duas vezes pelo mesmo tipo", mas permitir `REFUND` **e** `ROLLBACK` da mesma `BET` creditaria duas vezes. Uma referência admite uma única reversão `PROCESSED`. `ROLLBACK` de um `REFUND` continua possível, porque a referência é outra.
2. `WIN` e `LOSS` **podem** referenciar uma `BET` (opcional). Se referenciarem, valem as mesmas regras de escopo, tipo e status, e a referência ausente também gera `PENDING_REFERENCE`. `LOSS` aceita valor `0.00`; os demais tipos exigem valor > 0.
3. `ROLLBACK` aceita `BET`, `WIN` e `REFUND` como referência e aplica a direção inversa da referência.
4. **Wallet inexistente** responde `404 WALLET_NOT_FOUND` e **não é persistida**: sem wallet não há o que travar nem FK válida. Na fila, vai para a DLQ.
5. **Wallet de outro player** → `REJECTED / WALLET_PLAYER_MISMATCH` (persistida e auditável).
6. Uma referência que existe mas ainda está pendente conta como "ainda não disponível": a transação continua `PENDING_REFERENCE`.
7. Quando uma transação é processada ou rejeitada, o mesmo SQL "acorda" as transações `PENDING_REFERENCE` que a referenciam (`next_attempt_at = now()`). A resolução acontece em ~100 ms, sem esperar o próximo backoff.
8. Wallet criada com saldo `0.00` não gera `OPENING` nem lançamento.

### Transições de `WagerTransaction`

```
PENDING ──► PROCESSED | REJECTED | FAILED
   └─► PENDING_REFERENCE ──► PENDING_REFERENCE (nova tentativa agendada)
                        └──► PROCESSED | REJECTED | FAILED
```

Na prática a transação é liquidada em memória antes do primeiro `INSERT` e já nasce no estado final ou em `PENDING_REFERENCE`. `PENDING` existe apenas no objeto de domínio. Tentar transicionar um estado terminal lança `InvalidTransactionStateError` (erro de programação), e o trigger do banco rejeita o mesmo `UPDATE`.

### Referências fora de ordem (7.1)

- Backoff exponencial **persistido** (`next_attempt_at`, `reference_attempts`): 2 s, 4 s, 8 s … limitado a 5 min por espera, até 8 novas tentativas. São cerca de **13,5 min de TTL** no total. Justificativa: provedores costumam entregar a operação original em segundos; minutos cobrem uma fila parada ou um provedor reprocessando lote, sem deixar transações penduradas por horas.
- O worker roda em todas as instâncias. Reinício não perde agendamento (o estado está no banco), e instâncias concorrentes não duplicam efeito (lock da wallet + releitura do status).
- Esgotado o limite: `REJECTED / REFERENCE_NOT_FOUND` e evento `WagerTransactionRejected`.

## 7. Códigos de falha

| `failureCode` | Status | Situação | O provedor deve… |
|---|---|---|---|
| `INSUFFICIENT_FUNDS` | REJECTED | `BET` maior que o saldo | não reenviar |
| `REVERSAL_INSUFFICIENT_FUNDS` | REJECTED | reversão (ex.: `ROLLBACK` de `WIN`) deixaria o saldo negativo. **Código distinto**: exige ação operacional, não é um jogador sem saldo | escalar |
| `CURRENCY_MISMATCH` | REJECTED | moeda ≠ moeda da wallet | corrigir payload (nova operação) |
| `WALLET_PLAYER_MISMATCH` | REJECTED | `playerId` não é dono da wallet | corrigir payload |
| `REFERENCE_NOT_FOUND` | REJECTED | referência não chegou dentro do TTL | investigar |
| `REFERENCE_SCOPE_MISMATCH` | REJECTED | referência de outro provider/player/wallet/moeda/rodada | corrigir payload |
| `REFERENCE_KIND_NOT_ALLOWED` | REJECTED | ex.: `REFUND` de `WIN`, `ROLLBACK` de `LOSS` | corrigir payload |
| `REFERENCE_NOT_PROCESSED` | REJECTED | referência existe mas foi rejeitada | não reenviar |
| `REFERENCE_ALREADY_REVERSED` | REJECTED | referência já revertida | não reenviar |
| `REVERSAL_AMOUNT_MISMATCH` | REJECTED | valor ≠ valor da referência (reversão parcial está fora de escopo) | corrigir payload |
| `PROCESSING_FAILED` | FAILED | o worker de pendências teve um erro **não transitório** ao liquidar (bug/dados); a transação fica terminal e auditável | acionar suporte |

Erros que **não** viram transação persistida: `VALIDATION_ERROR` (400), `IDEMPOTENCY_CONFLICT` (409), `WALLET_NOT_FOUND` (404), `TRANSIENT_FAILURE` (503).

## 8. Mensageria

### Consumer ([`wager-consumer.ts`](src/infrastructure/messaging/wager-consumer.ts))

- Mesmo use case do HTTP. O registro de inbox `(consumerName, messageId)` é inserido **na mesma transação SQL** do efeito financeiro, já marcado como processado. Ele só passa a existir se o commit acontecer, então "existe no inbox" ⇔ "foi consumido".
- `ack` (DeleteMessage) **somente depois do commit**. O teste `5. worker killed after commit and before ack` mata o processo (SIGKILL) exatamente nesse intervalo; a redelivery cai no inbox e vira no-op.

| Classe de erro | Exemplos | Disposição |
|---|---|---|
| negócio | `REJECTED`, `PENDING_REFERENCE` | commit + ack |
| transitório | lock timeout, deadlock, conexão, pool esgotado | `ChangeMessageVisibility` com backoff `2s·2^(n-1)` (máx. 900 s) |
| permanente | JSON inválido, schema inválido, `IDEMPOTENCY_CONFLICT`, `WALLET_NOT_FOUND`, inbox com corpo divergente | envio explícito para a DLQ (com `errorCode`/`failureReason`) + delete |
| tentativas esgotadas | `ApproximateReceiveCount ≥ CONSUMER_MAX_RECEIVES` (5) | DLQ com `max_receives_exceeded` |

- A redrive policy da própria fila (`maxReceiveCount = 10`) é só um **backstop**, caso o consumer falhe até em mover a mensagem.
- **FIFO:** dentro de um lote, mensagens do mesmo `MessageGroupId` (= wallet) são processadas em ordem; grupos diferentes, em paralelo. Se uma mensagem do grupo não for confirmada, as seguintes do lote são devolvidas (visibilidade 0) para preservar a ordem. A ordem é otimização: a correção vem do banco, e mensagens fora de ordem caem no fluxo de `PENDING_REFERENCE`.
- **SIGTERM:** o consumer para de receber, termina o lote em andamento, devolve a visibilidade do que não começou, e só então os workers param e o pool fecha. O Nest re-emite o sinal no fim, então o código de saída é 143.

### Transactional Outbox ([`outbox-publisher.ts`](src/infrastructure/messaging/outbox-publisher.ts))

Os eventos são gravados na mesma transação SQL da mudança e nunca publicados antes do commit. O relay roda em todas as instâncias:

1. **claim:** `UPDATE … SET locked_by, locked_until WHERE id IN (SELECT … FOR UPDATE SKIP LOCKED LIMIT n)`. É uma transação curta; nenhuma transação fica aberta durante a chamada ao SQS.
2. **send:** `SendMessageBatch` (chunks de 10 em paralelo). Fila FIFO: `MessageGroupId = aggregateId`, `MessageDeduplicationId = eventId`.
3. **mark:** `published_at = now()` **somente se o lease ainda for nosso**.

Falha no SQS → `OutboxMessage.scheduleRetry()` (backoff exponencial 1 s → 5 min, com 20% de jitter). Se o processo morrer entre 2 e 3, o lease (30 s) expira e outra instância republica. A entrega é **at-least-once**: o consumidor deduplica por `eventId`, e na fila FIFO a janela de dedup do broker já absorve a maioria das repetições. Os testes usam uma fila de eventos *standard* justamente para que duplicatas fiquem visíveis, e provam que:

- **dois publishers concorrentes** publicam cada evento exatamente uma vez;
- **crash depois do commit e antes do publish:** outra instância publica;
- **crash depois do publish e antes do mark:** houve republicação, e as duplicatas são byte-a-byte idênticas.

| Evento | Quando |
|---|---|
| `WagerTransactionProcessed` | toda transação aplicada (inclusive `LOSS` e `OPENING`) |
| `WagerTransactionRejected` | toda rejeição (inclusive por TTL de referência) |
| `WalletBalanceChanged` | **só** quando há lançamento no ledger (1:1 com o ledger) |
| `WagerTransactionPendingReference` | uma vez, quando a transação é estacionada |

Cada evento é uma subclasse de `IntegrationEvent<T>` com `eventType` e `version` no tipo; `data` carrega `MoneyProps` (strings), nunca `Money`.

## 9. Observabilidade

- **Logs JSON (pino)** com `correlationId`, `messageId`, `transactionId`, `walletId` e `providerId`, propagados por `AsyncLocalStorage`. HTTP aceita/devolve `X-Correlation-Id`; na fila, o `messageId` é o `correlationId`. Payloads não são logados, e caminhos como `payload`, `body`, `data`, `money` e `authorization` são redigidos por segurança.
- **Métricas** (`GET /metrics`):

| Requisito | Métrica |
|---|---|
| transações por status | `wagering_transactions_total{kind,status,source}` |
| duplicatas | `wagering_duplicates_total{type=idempotent_replay\|inbox_redelivery\|idempotency_conflict}` |
| retries | `wagering_retries_total{operation,reason}` |
| DLQ | `sqs_dead_lettered_messages_total{reason}`, `sqs_dlq_depth` |
| conflitos de lock | `wagering_lock_conflicts_total{type=contended\|version\|unique_race\|deadlock}` |
| outbox lag | `outbox_lag_seconds`, `outbox_pending_messages`, `outbox_publish_phase_seconds{phase}` |
| latência | `wagering_processing_duration_seconds{source}` |
| extras | `sqs_consumer_messages_total{outcome}`, `wagering_reconciliations_total{result}`, `wagering_pending_reference_transactions` |

- **Health:** `/health/live` só responde (não checa dependências, para não causar restart em cascata quando o banco oscila); `/health/ready` checa Postgres (`select 1`) e SQS (`GetQueueAttributes`) com timeout de 2 s.
- **Reconciliação:** `FOR SHARE` na wallet (writers esperam) + soma do ledger no Postgres. Divergência é logada em `error`, conta em `wagering_reconciliations_total{result="divergent"}` e volta com `consistent: false`. Nunca é corrigida automaticamente.

## 10. Autenticação (seção 2): não implementada, por decisão

O enunciado diz que autenticação não vale pontos e não deve competir com correção financeira; o tempo foi para concorrência, idempotência e falhas. O ponto de extensão está no código:

- [`ProviderIdentityPort`](src/infrastructure/auth/provider-auth.guard.ts): `authenticate(authorizationHeader) → ProviderIdentity | undefined`;
- `ProviderAuthGuard` global (`APP_GUARD`), com `@Public()` em health e métricas;
- implementação atual: `TrustAllProviderIdentity` (no-op).

**Desenho que eu adotaria:** Keycloak no compose, com um *client* confidencial por provedor no fluxo **client credentials** (máquina-a-máquina). O token JWT carrega a claim `provider_ids`. A implementação do port valida assinatura via **JWKS** (com cache), `iss`, `aud` e `exp`. O guard compara `body.providerId` (e `:providerId` na rota) com a claim e responde `403` se não bater, o que impede um provedor de operar em nome de outro. A fila continua sendo canal interno confiável, mas a identidade do provedor dentro da mensagem passa pelas mesmas validações de domínio.

## 11. Testes

| Suíte | O que prova |
|---|---|
| `unit` (72) | `Money` (escala, inválidos, exatidão, moeda), invariantes da `Wallet`, ledger imutável, todas as regras e transições, backoff, envelope dos eventos, idempotência e inbox no use case |
| `integration` (44) | migrations reversíveis; **cada constraint/trigger** violada diretamente em SQL; API (mapa de status, replay, cursores, reconciliação, health, métricas); consumer (inbox, redelivery, DLQ por motivo, retry por lock timeout, max receives); **atomicidade** (falha real na escrita do outbox desfaz transação, ledger, wallet e inbox, pelo HTTP e pela fila); outbox (envelope, falha do SQS e backoff) |
| `concurrency` (12) | os 8 cenários da seção 13, com **paralelismo real**: 3 apps com pools próprios, **processos separados** (`bun src/main.ts`), SIGKILL/SIGTERM e injeção de falha (`FAULT_INJECTION`) |

Todos os testes de integração e concorrência terminam com `assertGlobalInvariants()`, que verifica, para **todas** as wallets do banco do teste:

- `balance == Σ ledger`;
- `version == última versão do ledger`;
- encadeamento íntegro;
- nenhum saldo negativo;
- exatamente um lançamento por transação `PROCESSED` que move saldo.

Não há mock de Postgres ou SQS fora dos testes de unidade.

## 12. Teste de carga

Detalhes e números em [docs/LOAD_TEST.md](docs/LOAD_TEST.md). Em resumo, numa máquina única (12 cores) com tudo junto: **~300–380 req/s sem hot wallets** (p99 0,5–0,8 s) e **~130–190 req/s com 20% do tráfego em 2 wallets quentes** (p99 ~2–2,8 s), com **0 erros, 0 eventos duplicados e 0 divergências** na reconciliação de todas as wallets. O lag do outbox sob carga (até ~50 s) é dominado pelo SQS emulado, e isso foi medido e explicado no relatório.

## 13. Trade-offs e limitações conhecidas

- **Hot wallet tem teto de vazão:** todas as operações de uma wallet se serializam por desenho. Escalaria com *sharding* do saldo ou com sub-contas por provedor; fora de escopo.
- **Ledger de partida simples:** cada lançamento afeta uma wallet. Partidas dobradas (contas de contrapartida da casa) eram opcionais.
- **Sem retenção:** `inbox_messages` e `outbox_messages` publicados crescem sem limite. Em produção: job de purga por idade (inbox ≥ janela de redelivery; outbox publicado ≥ dias) ou particionamento por tempo.
- **Ordem de eventos entre publishers concorrentes:** o FIFO por `aggregateId` não garante ordem global se duas instâncias publicarem eventos da mesma wallet ao mesmo tempo. Consumidores devem usar `walletVersion` (presente em `WalletBalanceChanged`) e deduplicar por `eventId`.
- **Reversão parcial** e **múltiplas reversões** da mesma referência estão fora de escopo (rejeitadas com códigos próprios).
- **`FAILED`** só é produzido pelo worker de pendências. Falhas de infraestrutura na entrada não chegam a persistir a transação (rollback) e são devolvidas como 503 / retry / DLQ.
- **Reconciliação sob demanda:** não há job periódico; seria um cron chamando o mesmo use case por lote de wallets.
- **Autenticação:** ver seção 10.
- **MiniStack:** emulador de processo único; a vazão degrada com a profundidade da fila. Números de outbox e fila não refletem o SQS real.
