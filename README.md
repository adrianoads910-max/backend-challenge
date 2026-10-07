# Distributed Wagering Processor

Serviço financeiro que processa transações de apostas (`BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`) vindas de vários provedores, por HTTP e por SQS. Ele continua correto quando as mensagens chegam **duplicadas**, **fora de ordem** ou **ao mesmo tempo em várias instâncias**.

- Enunciado original: [docs/CHALLENGE.md](docs/CHALLENGE.md)
- Decisões, trade-offs e limitações: [ARCHITECTURE.md](ARCHITECTURE.md)
- Teste de carga (metodologia e resultados): [docs/LOAD_TEST.md](docs/LOAD_TEST.md)

**Stack:** Bun 1.3 · TypeScript strict · NestJS 11 · MikroORM 6 · PostgreSQL 18 · SQS (MiniStack) · Docker Compose.

---

## Subir tudo

Pré-requisitos: Docker com Compose v2 e Bun ≥ 1.3 (o Bun só é necessário para rodar testes e scripts fora do container).

```bash
docker compose up -d --build
```

O comando sobe o PostgreSQL, o MiniStack (SQS), um container `migrate` que aplica as migrations e **três instâncias idênticas** da aplicação. Cada instância serve HTTP, consome a fila, publica o outbox e resolve referências pendentes.

| Serviço | Endereço |
|---|---|
| app-1 / app-2 / app-3 | http://localhost:3101 · :3102 · :3103 |
| PostgreSQL | `postgres://wagering:wagering@localhost:5442/wagering` |
| MiniStack (SQS) | http://localhost:4566 |

As filas `wager-transactions.fifo`, `wager-transactions-dlq.fifo` e `wagering-events.fifo` são criadas no boot (`SQS_AUTO_CREATE_QUEUES=true`).

### Rodar a aplicação fora do Docker

```bash
bun install
bun run infra:up        # só postgres + ministack
bun run migrate         # aplica migrations
bun run start           # http://localhost:3000
```

## Comandos

| Comando | O que faz |
|---|---|
| `bun run start` / `bun run dev` | sobe a aplicação (dev = watch) |
| `bun run migrate` / `bun run migrate:down` | aplica / reverte a última migration |
| `bun run typecheck` | `tsc --noEmit` em modo estrito |
| `bun run test` | todas as suítes (unidade → integração → concorrência) |
| `bun run test:unit` | domínio e aplicação, sem I/O (~0,1 s) |
| `bun run test:integration` | Postgres e SQS reais (~40 s) |
| `bun run test:concurrency` | paralelismo real, múltiplos processos, crashes (~75 s) |
| `bun run test:load` | teste de carga contra as instâncias do compose |

Os testes de integração e de concorrência precisam de Postgres e MiniStack de pé (`bun run infra:up`). Cada arquivo de teste cria **seu próprio banco e suas próprias filas** e os remove ao final, então os testes não interferem entre si nem com o ambiente do compose.

## API

Todos os valores monetários são strings decimais com 2 casas: `{ "amount": "25.00", "currency": "BRL" }`.

| Método | Rota | Notas |
|---|---|---|
| `POST` | `/wallets` | `{ playerId, initialBalance }`. Saldo > 0 gera transação `OPENING` + crédito no ledger, na mesma transação SQL |
| `GET` | `/wallets/:walletId` | |
| `GET` | `/wallets/:walletId/ledger?cursor=&limit=50` | cursor opaco e estável (versão da wallet) |
| `POST` | `/wallets/:walletId/reconciliation` | recalcula o saldo pelo ledger; divergência é logada, contada e sinalizada, nunca corrigida |
| `POST` | `/wagering/transactions` | header `Idempotency-Key` obrigatório |
| `GET` | `/wagering/transactions/:transactionId` | |
| `GET` | `/providers/:providerId/wagering/transactions/:externalTransactionId` | |
| `GET` | `/health/live` · `/health/ready` | abertos; o ready checa Postgres e SQS |
| `GET` | `/metrics` | Prometheus |

### Códigos HTTP (iguais em todos os endpoints)

| Status | Significado | O provedor deve… |
|---|---|---|
| `200` | transação `PROCESSED` (primeira vez ou replay) | seguir |
| `202` | aceita, `PENDING_REFERENCE` (referência ainda não chegou) | consultar depois ou reenviar (replay) |
| `400` | payload inválido (`VALIDATION_ERROR`) | corrigir o payload |
| `404` | wallet/transação inexistente | corrigir a referência |
| `409` | `IDEMPOTENCY_CONFLICT` (mesma key, outro payload) ou `WALLET_ALREADY_EXISTS` | não reenviar |
| `422` | regra de negócio: transação `REJECTED` + `failureCode` | não reenviar |
| `503` | falha transitória de infraestrutura (`Retry-After`) | reenviar com a mesma key |
| `500` | erro inesperado / transação `FAILED` | acionar suporte (reenviar é seguro) |

O replay devolve **o mesmo status e o mesmo corpo** da primeira resposta, inclusive o saldo observado naquele momento, com `idempotentReplay: true` e o header `Idempotent-Replay: true`.

### Exemplo

```bash
W=$(curl -s -XPOST localhost:3101/wallets -H 'content-type: application/json' \
  -d '{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"100.00","currency":"BRL"}}' | jq -r .id)

curl -s -XPOST localhost:3102/wagering/transactions \
  -H 'content-type: application/json' -H 'Idempotency-Key: provider-a:transaction-123' \
  -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"transaction-123\",
       \"playerId\":\"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1\",\"walletId\":\"$W\",
       \"roundId\":\"round-987\",\"gameId\":\"fortune-chimp\",\"kind\":\"BET\",
       \"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"}}"
# {"transactionId":"…","status":"PROCESSED","balance":{"amount":"75.00","currency":"BRL"},"idempotentReplay":false}
```

### Pela fila

Envie para `wager-transactions.fifo` o envelope `WagerTransactionRequested` da seção 10 do enunciado, com `MessageGroupId = walletId`. O consumidor usa **o mesmo use case** do HTTP.

## Configuração

Todas as variáveis têm default (veja [src/config.ts](src/config.ts)). As mais relevantes:

| Variável | Default | |
|---|---|---|
| `DATABASE_URL` | `postgres://wagering:wagering@localhost:5442/wagering` | |
| `SQS_ENDPOINT` | `http://localhost:4566` | |
| `DB_LOCK_TIMEOUT_MS` | `5000` | espera máxima pelo lock de uma wallet → 503 |
| `CONSUMER_MAX_RECEIVES` | `5` | tentativas antes da DLQ |
| `PENDING_REFERENCE_MAX_ATTEMPTS` | `8` | com backoff 2 s → 5 min, ≈ 13,5 min de TTL |
| `ENABLE_HTTP` / `ENABLE_CONSUMER` / `ENABLE_OUTBOX_PUBLISHER` / `ENABLE_PENDING_REFERENCE_WORKER` | `true` | papéis de cada instância |
| `LOG_LEVEL` | `info` | logs JSON (pino) |

## Estrutura

```
src/
  domain/           Money, Wallet, WagerTransaction, WalletLedgerEntry, Inbox/Outbox, eventos, regras (wager-settlement)
  application/      use cases + portas (UnitOfWork, repositórios, métricas, relógio, ids)
  infrastructure/   MikroORM (schema/migrations/repositórios), SQS (consumer, outbox), workers, observabilidade, auth
  interface/        HTTP (controllers, mapeamento de erros) e contratos de entrada (zod)
test/
  unit/             domínio e use case com adaptadores em memória
  integration/      schema, API, consumer, outbox, atomicidade — Postgres e SQS reais
  concurrency/      os 8 cenários da seção 13 com paralelismo real e processos separados
scripts/load-test.ts
```
