# Teste de carga

```bash
docker compose up -d --build
bun run test:load                                   # 60 s, 64 clientes, 20% do tráfego em hot wallets
LOAD_HOT_SHARE=0 LOAD_DURATION_S=60 bun run test:load
```

Script: [scripts/load-test.ts](../scripts/load-test.ts). Parâmetros: `LOAD_TARGETS`, `LOAD_DURATION_S`, `LOAD_CONCURRENCY`, `LOAD_WALLETS`, `LOAD_HOT_WALLETS`, `LOAD_HOT_SHARE`, `LOAD_EVENTS_QUEUE`.

## Ambiente

Uma única máquina Linux (12 vCPU, 15 GB RAM). **Tudo** roda nela ao mesmo tempo: PostgreSQL 18, MiniStack, as 3 instâncias da aplicação (Docker, pool de 10 conexões cada) e o próprio gerador de carga (Bun 1.3.13). Os números são, portanto, um limite inferior e não representam uma topologia de produção: banco, broker e aplicação disputam CPU entre si.

## Metodologia

- **Preparação:** 200 wallets "normais" e 2 "quentes", todas com saldo de 1.000.000,00, para que a carga meça o caminho de escrita e não rejeições por saldo.
- **Carga:** 64 clientes concorrentes durante 60 s, distribuídos aleatoriamente entre as 3 instâncias.
  - Mix: 70% `BET 1.00`, 25% `WIN 1.50`, 5% `LOSS`.
  - 5% das requisições são **replays deliberados** de operações já enviadas.
  - Uma fração (`LOAD_HOT_SHARE`) do tráfego vai para as 2 hot wallets.
- **Coleta:**
  - latência por requisição (p50/p95/p99/max) e status;
  - antes/depois em `/metrics` das 3 instâncias: conflitos de lock, duplicatas detectadas;
  - `outbox_lag_seconds` amostrado a cada 1 s.
- **Consumidor de eventos:** um consumidor downstream (8 loops) consome e deleta a fila de eventos e conta `eventId`s únicos e repetidos.
- **Pós-carga:** espera a drenagem completa do outbox (até 5 min) e roda a **reconciliação de todas as 202 wallets**. Qualquer divergência faz o script sair com código 1.

## Resultados

| Cenário | Requisições | Vazão | p50 | p95 | p99 | max | Erros | Replays detectados | Lock contended | Divergências |
|---|---|---|---|---|---|---|---|---|---|---|
| sem hot wallets (run 1) | 19.702 | 327 rps | 171 ms | 427 ms | 621 ms | 0,9 s | 0 | 999 / 999 | 1.795 | 0 / 202 |
| sem hot wallets (run 2*) | 18.100 | 296 rps | 173 ms | 523 ms | 834 ms | 1,3 s | 0 | 900 / 900 | 1.680 | 0 / 202 |
| 20% em 2 hot wallets (run 1) | 11.781 | 193 rps | 223 ms | 965 ms | 2,06 s | 3,5 s | 0 | 600 / 600 | 2.403 | 0 / 202 |
| 20% em 2 hot wallets (run 2*) | 8.211 | 130 rps | 315 ms | 1,53 s | 2,76 s | 7,7 s | 0 | 373 / 373 | 1.768 | 0 / 202 |

\* Run 2 com o consumidor downstream de eventos ligado, competindo pela mesma CPU e pelo mesmo MiniStack.

| Cenário | Outbox lag máx / médio | Drenagem pós-carga | Eventos entregues ao consumidor | Duplicatas de evento |
|---|---|---|---|---|
| sem hot (run 2) | 50,8 s / 24,1 s | 274 s | 11.733 durante a janela | **0** |
| 20% hot (run 2) | 47,8 s / 22,8 s | 104 s | 4.819 durante a janela | **0** |

**Taxa de erro: 0** em todos os runs: todas as respostas foram 200. Não houve nenhum 503, porque o `lock_timeout` de 5 s não foi atingido.

## Análise

**Correção sob carga.** Em todos os runs:
- todo replay deliberado foi reconhecido (`wagering_duplicates_total` = replays enviados);
- nenhum evento chegou duplicado;
- o outbox drenou por completo;
- a reconciliação de todas as wallets fechou com diferença `0.00`.

Os triggers diferidos do schema também validaram cada commit, ou seja, uma divergência teria virado erro, e não houve nenhum.

**Hot wallets custam o que deveriam custar.** Com 20% do tráfego concentrado em 2 wallets, a vazão cai ~40–55% e o p99 sobe para 2–3 s. Esse é o efeito esperado do lock por wallet: as operações dessas 2 wallets se serializam, e cada uma segura uma conexão do pool enquanto espera, o que atrasa também as wallets normais. O `lock_timeout` (5 s) e o 503 retentável existem justamente para limitar isso (veja o teste `hot wallet` na suíte de concorrência). Para provedores com wallets muito quentes, o próximo passo seria isolar pools ou particionar o saldo (ARCHITECTURE §13).

**Lag do outbox: o gargalo é o broker emulado, não o relay.** A investigação, passo a passo:

1. **Fases do relay** (histograma `outbox_publish_phase_seconds`, sob carga): *claim* ~47 ms, *mark* ~97 ms, **send ~2,2 s** por lote de 100 eventos. O banco não é o gargalo.
2. **Relay com banco ocioso:** um backlog sintético de 6.000 eventos drena em 4,4 s, cerca de 1.360 eventos/s nas 3 instâncias. O plano do *claim* (`FOR UPDATE SKIP LOCKED` + índice parcial) executa em 0,5 ms.
3. **MiniStack:** `SendMessageBatch` numa fila FIFO vazia alcança **~9.100 msg/s**; na fila de eventos com 83 mil mensagens acumuladas, **~140 msg/s**. O emulador degrada linearmente com a profundidade da fila. Sob carga, um único processo do MiniStack atende ao mesmo tempo os envios dos 3 publishers, os long-polls dos 3 consumers e o consumidor de eventos, e não acompanha os ~600–700 eventos/s gerados.
4. A primeira versão do relay enviava os chunks de 10 em sequência. Passou a enviá-los em paralelo, e o lote padrão subiu para 100. Isso melhorou o relay isolado, mas não muda o teto imposto pelo emulador.

Conclusão: com SQS real (sem degradação por profundidade e com vazão muito maior), o lag esperado seria da ordem do intervalo de polling (100–500 ms) mais a latência de rede. Não foi possível medir isso aqui. O valor operacional do experimento está nas métricas: `outbox_lag_seconds`, `outbox_pending_messages` e `outbox_publish_phase_seconds` mostram com precisão onde está o atraso.

**Limitações do experimento.** Gerador de carga e sistema na mesma máquina; um único run de 60 s por cenário (com variância visível entre os runs 1 e 2); SQS emulado; sem aquecimento separado da medição.
