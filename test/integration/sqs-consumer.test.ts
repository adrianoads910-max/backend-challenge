import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { eventually, type Instance, sleep, TestEnv, wager } from "../support/env";

let env: TestEnv;
let app: Instance;

beforeAll(async () => {
  env = await TestEnv.create("sqs");
  app = await env.startInProcess({
    DB_LOCK_TIMEOUT_MS: "300",
    CONSUMER_MAX_RECEIVES: "3",
    CONSUMER_RETRY_BASE_S: "1",
    CONSUMER_VISIBILITY_TIMEOUT_S: "5",
    ENABLE_OUTBOX_PUBLISHER: "false",
  });
});
afterAll(async () => {
  await env.assertGlobalInvariants();
  await env.dispose();
});

async function txStatus(externalId: string) {
  const [row] = await env.sql`select status, failure_code from wager_transactions where external_transaction_id = ${externalId}`;
  if (!row) throw new Error("not yet");
  return row as { status: string; failure_code: string | null };
}

const dlqMessages = async () =>
  (await env.receiveAll(env.queues.dlq, { waitMs: 2_000, visibility: 120 })).map((m) => ({
    body: m.body,
    code: m.attributes.errorCode,
    reason: m.attributes.failureReason,
  }));

async function untilQueueEmpty() {
  await eventually(async () => {
    const d = await env.queueDepth(env.queues.wager);
    if (d.visible + d.inFlight > 0) throw new Error(JSON.stringify(d));
  }, { timeoutMs: 30_000 });
}

describe("SQS consumer", () => {
  test("processes a message through the same use case and acks after commit", async () => {
    const w = await env.createWallet(app.baseUrl, "100.00");
    const bet = wager(w, "BET", "25.00");
    const messageId = await env.sendWagerMessage(bet);
    await eventually(() => txStatus(bet.externalTransactionId));
    await untilQueueEmpty();
    expect((await env.walletRow(w.walletId)).balance).toBe("75.00");
    const inbox = await env.sql`select consumer_name, processed_at from inbox_messages where message_id = ${messageId}`;
    expect(inbox).toHaveLength(1);
    expect(inbox[0].processed_at).not.toBeNull();
  });

  test("redelivery of the same messageId is a no-op (persistent inbox)", async () => {
    const w = await env.createWallet(app.baseUrl, "100.00");
    const bet = wager(w, "BET", "10.00");
    const messageId = crypto.randomUUID();
    for (let i = 0; i < 3; i++) await env.sendWagerMessage(bet, messageId); // distinct SQS dedup ids → 3 deliveries
    await untilQueueEmpty();
    expect(await env.ledgerRows(w.walletId)).toHaveLength(2);
    expect((await env.walletRow(w.walletId)).balance).toBe("90.00");
    const metrics = await (await fetch(`${app.baseUrl}/metrics`)).text();
    expect(metrics).toMatch(/sqs_consumer_messages_total\{outcome="duplicate"\} [2-9]/);
  });

  test("the same operation under a new messageId is an idempotent replay (no double debit)", async () => {
    const w = await env.createWallet(app.baseUrl, "100.00");
    const bet = wager(w, "BET", "10.00");
    await env.sendWagerMessage(bet);
    await env.sendWagerMessage(bet);
    const http = await env.submit(app.baseUrl, bet);
    await untilQueueEmpty();
    expect(http.body.idempotentReplay || http.status === 200).toBe(true);
    expect(await env.ledgerRows(w.walletId)).toHaveLength(2);
  });

  test("business rejection is terminal: acked, not dead-lettered", async () => {
    const w = await env.createWallet(app.baseUrl, "1.00");
    const bet = wager(w, "BET", "50.00");
    await env.sendWagerMessage(bet);
    expect(await eventually(() => txStatus(bet.externalTransactionId))).toEqual({ status: "REJECTED", failure_code: "INSUFFICIENT_FUNDS" });
    await untilQueueEmpty();
  });

  test("permanent errors go to the DLQ with a reason; nothing is persisted", async () => {
    const w = await env.createWallet(app.baseUrl, "100.00");
    await env.sendWagerMessage(wager(w, "BET", "1.00"), crypto.randomUUID(), { raw: "{not json" });
    await env.sendWagerMessage(wager(w, "BET", "1.001"));
    await env.sendWagerMessage(wager({ ...w, walletId: crypto.randomUUID() }, "BET", "1.00"));
    const original = wager(w, "BET", "1.00");
    await env.sendWagerMessage(original);
    await eventually(() => txStatus(original.externalTransactionId));
    await env.sendWagerMessage({ ...original, money: { amount: "2.00", currency: "BRL" } }); // idempotency conflict
    await untilQueueEmpty();
    const dlq = await dlqMessages();
    expect(dlq.map((m) => m.code).sort()).toEqual(["IDEMPOTENCY_CONFLICT", "MALFORMED_MESSAGE", "VALIDATION_ERROR", "WALLET_NOT_FOUND"]);
    expect(dlq.every((m) => m.reason === "permanent" || m.code === "MALFORMED_MESSAGE")).toBe(true);
    expect((await env.walletRow(w.walletId)).balance).toBe("99.00");
  });

  test("transient failure (wallet lock timeout) is retried with backoff and then succeeds", async () => {
    const w = await env.createWallet(app.baseUrl, "100.00");
    const holder = await env.sql.reserve();
    await holder`begin`;
    await holder`select id from wallets where id = ${w.walletId} for update`;
    const bet = wager(w, "BET", "5.00");
    await env.sendWagerMessage(bet);
    await sleep(1_500); // at least one lock_timeout + visibility backoff
    await holder`rollback`;
    holder.release();
    expect(await eventually(() => txStatus(bet.externalTransactionId), { timeoutMs: 15_000 })).toEqual({ status: "PROCESSED", failure_code: null });
    await untilQueueEmpty();
    const metrics = await (await fetch(`${app.baseUrl}/metrics`)).text();
    expect(metrics).toMatch(/wagering_retries_total\{operation="sqs_consume",reason="lock_timeout"\} \d/);
    expect(metrics).toMatch(/sqs_consumer_messages_total\{outcome="retried"\} \d/);
  });

  test("a message that keeps failing transiently is dead-lettered after CONSUMER_MAX_RECEIVES", async () => {
    await dlqMessages(); // drain previous
    const w = await env.createWallet(app.baseUrl, "100.00");
    const holder = await env.sql.reserve();
    await holder`begin`;
    await holder`select id from wallets where id = ${w.walletId} for update`;
    const bet = wager(w, "BET", "5.00");
    await env.sendWagerMessage(bet);
    try {
      await eventually(async () => {
        const d = await dlqMessages();
        if (d.length === 0) throw new Error("not in DLQ yet");
        expect(d[0]!.reason).toBe("max_receives_exceeded");
      }, { timeoutMs: 20_000 });
    } finally {
      await holder`rollback`;
      holder.release();
    }
    const rows = await env.sql`select 1 from wager_transactions where external_transaction_id = ${bet.externalTransactionId}`;
    expect(rows).toHaveLength(0);
  });
});

describe("atomicity", () => {
  test("a failure while writing the outbox rolls back transaction, ledger, wallet and inbox together", async () => {
    const w = await env.createWallet(app.baseUrl, "100.00");
    // Make the last write of the business transaction (outbox) fail.
    await env.sql.unsafe(`alter table outbox_messages add constraint test_block_balance_events check (event_type <> 'WalletBalanceChanged') not valid`);
    const bet = wager(w, "BET", "40.00");
    const messageId = await env.sendWagerMessage(bet);
    try {
      await sleep(2_500); // at least one failed attempt
      expect(await env.sql`select 1 from wager_transactions where external_transaction_id = ${bet.externalTransactionId}`).toHaveLength(0);
      expect(await env.sql`select 1 from inbox_messages where message_id = ${messageId}`).toHaveLength(0);
      expect(await env.ledgerRows(w.walletId)).toHaveLength(1);
      expect(await env.walletRow(w.walletId)).toEqual({ balance: "100.00", version: 1 });
      // the HTTP path behaves the same way
      const http = await env.submit(app.baseUrl, wager(w, "BET", "1.00"));
      expect(http.status).toBe(500);
      expect(await env.ledgerRows(w.walletId)).toHaveLength(1);
    } finally {
      await env.sql.unsafe(`alter table outbox_messages drop constraint test_block_balance_events`);
    }
    // redelivery now succeeds, exactly once
    expect((await eventually(() => txStatus(bet.externalTransactionId), { timeoutMs: 15_000 })).status).toBe("PROCESSED");
    await untilQueueEmpty();
    expect(await env.walletRow(w.walletId)).toEqual({ balance: "60.00", version: 2 });
    const events = await env.sql`select event_type from outbox_messages where payload->'data'->>'walletId' = ${w.walletId} and payload->'data'->>'transactionId' in (select id::text from wager_transactions where external_transaction_id = ${bet.externalTransactionId}) order by seq`;
    expect(events.map((e: any) => e.event_type)).toEqual(["WalletBalanceChanged", "WagerTransactionProcessed"]);
  });
});
