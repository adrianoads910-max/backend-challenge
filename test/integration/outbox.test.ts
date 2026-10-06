import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import type { MikroORM } from "@mikro-orm/postgresql";
import type { INestApplication } from "@nestjs/common";
import { loadConfig } from "../../src/config";
import { OutboxPublisher } from "../../src/infrastructure/messaging/outbox-publisher";
import { PinoAppLogger } from "../../src/infrastructure/observability/logger";
import { PromMetrics } from "../../src/infrastructure/observability/metrics";
import { ORM } from "../../src/interface/http/health.controller";
import { eventually, type Instance, TestEnv, wager } from "../support/env";

let env: TestEnv;
let api: Instance & { app: INestApplication };

beforeAll(async () => {
  env = await TestEnv.create("outbox", { standardEvents: true });
  // Publisher disabled: tests drive publishing explicitly.
  api = await env.startInProcess({ ENABLE_CONSUMER: "false", ENABLE_OUTBOX_PUBLISHER: "false" });
});
afterAll(async () => {
  await env.assertGlobalInvariants();
  await env.dispose();
});

function publisher(queueUrl: string, instanceId: string) {
  const config = loadConfig(env.appEnv({ OUTBOX_BATCH_SIZE: "50" }));
  return new OutboxPublisher(api.app.get<MikroORM>(ORM), env.sqs, queueUrl, config, new PinoAppLogger("silent", {}), new PromMetrics(), instanceId);
}

describe("transactional outbox", () => {
  test("nothing is published before commit; publishing relays the exact envelope", async () => {
    const w = await env.createWallet(api.baseUrl, "100.00");
    const bet = await env.submit(api.baseUrl, wager(w, "BET", "10.00"));
    await env.submit(api.baseUrl, wager(w, "LOSS", "0.00"));
    expect(await env.queueDepth(env.queues.events)).toEqual({ visible: 0, inFlight: 0 });

    const p = publisher(await env.queueUrl(env.queues.events), "pub-1");
    while ((await p.publishBatch()) > 0);
    const messages = (await env.receiveAll(env.queues.events, { waitMs: 2_000 })).map((m) => JSON.parse(m.body));
    const forWallet = messages.filter((m) => m.aggregateId === w.walletId);
    // opening: WalletBalanceChanged + Processed; BET: WalletBalanceChanged + Processed; LOSS: Processed only
    expect(forWallet.map((m) => m.eventType).sort()).toEqual([
      "WagerTransactionProcessed", "WagerTransactionProcessed", "WagerTransactionProcessed",
      "WalletBalanceChanged", "WalletBalanceChanged",
    ]);
    const changed = forWallet.find((m) => m.eventType === "WalletBalanceChanged" && m.data.transactionId === bet.body.transactionId);
    expect(changed).toMatchObject({
      version: 1,
      correlationId: expect.any(String),
      occurredAt: expect.stringMatching(/Z$/),
      data: {
        walletId: w.walletId,
        direction: "DEBIT",
        money: { amount: "10.00", currency: "BRL" },
        balanceBefore: { amount: "100.00", currency: "BRL" },
        balanceAfter: { amount: "90.00", currency: "BRL" },
        walletVersion: 2,
      },
    });
    const pending = await env.sql`select count(*)::int as n from outbox_messages where published_at is null`;
    expect(pending[0].n).toBe(0);
  });

  test("SQS failure reschedules with backoff and keeps the event until it is published", async () => {
    const w = await env.createWallet(api.baseUrl, "5.00");
    const broken = publisher("http://localhost:4566/000000000000/does-not-exist", "pub-broken");
    expect(await broken.publishBatch()).toBe(0);
    const [row] = await env.sql`select attempts, next_attempt_at > now() as later, last_error is not null as has_error, locked_by
                                  from outbox_messages where aggregate_id = ${w.walletId} limit 1`;
    expect(row).toEqual({ attempts: 1, later: true, has_error: true, locked_by: null });

    const good = publisher(await env.queueUrl(env.queues.events), "pub-good");
    expect(await good.publishBatch()).toBe(0); // not due yet (backoff)
    await eventually(async () => {
      await good.publishBatch();
      const [r] = await env.sql`select count(*)::int as n from outbox_messages where aggregate_id = ${w.walletId} and published_at is null`;
      if (r.n !== 0) throw new Error(`${r.n} pending`);
    }, { timeoutMs: 10_000, intervalMs: 300 });
  });
});
