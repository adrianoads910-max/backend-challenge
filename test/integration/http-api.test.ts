import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { eventually, type Instance, TestEnv, wager } from "../support/env";

let env: TestEnv;
let api: Instance;

beforeAll(async () => {
  env = await TestEnv.create("http");
  api = await env.startInProcess({ ENABLE_CONSUMER: "false", PENDING_REFERENCE_MAX_ATTEMPTS: "3" });
});
afterAll(async () => {
  await env.assertGlobalInvariants();
  await env.dispose();
});

const get = async (path: string) => {
  const res = await fetch(`${api.baseUrl}${path}`);
  return { status: res.status, body: (await res.json()) as any };
};

describe("wallets", () => {
  test("create returns version 1 and books the OPENING credit atomically", async () => {
    const w = await env.createWallet(api.baseUrl, "1000.00");
    const wallet = await get(`/wallets/${w.walletId}`);
    expect(wallet.body.balance).toEqual({ amount: "1000.00", currency: "BRL" });
    expect(wallet.body.version).toBe(1);
    const ledger = await env.ledgerRows(w.walletId);
    expect(ledger).toHaveLength(1);
    expect(ledger[0]).toMatchObject({ direction: "CREDIT", amount: "1000.00", before: "0.00", after: "1000.00", version: 1 });
    const [opening] = await env.sql`select kind, status from wager_transactions where id = ${ledger[0]!.transaction_id}`;
    expect(opening).toEqual({ kind: "OPENING", status: "PROCESSED" });
  });

  test("duplicate player+currency is 409, invalid money is 400", async () => {
    const w = await env.createWallet(api.baseUrl);
    const dup = await fetch(`${api.baseUrl}/wallets`, {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ playerId: w.playerId, initialBalance: { amount: "1.00", currency: "BRL" } }),
    });
    expect(dup.status).toBe(409);
    expect(((await dup.json()) as any).error.code).toBe("WALLET_ALREADY_EXISTS");
    const bad = await fetch(`${api.baseUrl}/wallets`, {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ playerId: "p", initialBalance: { amount: 10, currency: "BRL" } }),
    });
    expect(bad.status).toBe(400);
  });

  test("404 for unknown wallet / transaction, 400 for malformed ids", async () => {
    expect((await get(`/wallets/${crypto.randomUUID()}`)).status).toBe(404);
    expect((await get(`/wallets/not-a-uuid`)).status).toBe(400);
    expect((await get(`/wagering/transactions/${crypto.randomUUID()}`)).status).toBe(404);
    expect((await get(`/providers/x/wagering/transactions/y`)).status).toBe(404);
  });
});

describe("POST /wagering/transactions", () => {
  test("status mapping: 200 processed, 422 rejected, 202 pending, 409 conflict, 400 invalid, 404 wallet", async () => {
    const w = await env.createWallet(api.baseUrl, "100.00");
    const bet = wager(w, "BET", "80.00");
    expect((await env.submit(api.baseUrl, bet)).status).toBe(200);
    const poor = await env.submit(api.baseUrl, wager(w, "BET", "80.00"));
    expect(poor.status).toBe(422);
    expect(poor.body).toMatchObject({ status: "REJECTED", failureCode: "INSUFFICIENT_FUNDS", balance: { amount: "20.00" } });
    const pending = await env.submit(api.baseUrl, wager(w, "REFUND", "5.00", { referenceExternalTransactionId: "nope" }));
    expect(pending.status).toBe(202);
    expect(pending.body.status).toBe("PENDING_REFERENCE");
    const conflict = await env.submit(api.baseUrl, { ...bet, money: { amount: "1.00", currency: "BRL" } });
    expect(conflict.status).toBe(409);
    expect(conflict.body.error?.code).toBe("IDEMPOTENCY_CONFLICT");
    expect((await env.submit(api.baseUrl, { ...bet, kind: "OPENING" as any }, "k-open")).status).toBe(400);
    expect((await env.submit(api.baseUrl, wager(w, "BET", "1.001"))).status).toBe(400);
    expect((await env.submit(api.baseUrl, wager(w, "REFUND", "1.00"))).status).toBe(400);
    expect((await env.submit(api.baseUrl, wager({ ...w, walletId: crypto.randomUUID() }, "BET", "1.00"))).status).toBe(404);
  });

  test("Idempotency-Key header is mandatory", async () => {
    const w = await env.createWallet(api.baseUrl);
    const res = await fetch(`${api.baseUrl}/wagering/transactions`, {
      method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(wager(w, "BET", "1.00")),
    });
    expect(res.status).toBe(400);
  });

  test("replay returns the original response (status, balance) with idempotentReplay=true", async () => {
    const w = await env.createWallet(api.baseUrl, "100.00");
    const bet = wager(w, "BET", "30.00");
    const first = await env.submit(api.baseUrl, bet);
    await env.submit(api.baseUrl, wager(w, "WIN", "500.00"));
    const replay = await env.submit(api.baseUrl, bet);
    expect(replay.status).toBe(first.status);
    expect(replay.body).toEqual({ ...first.body, idempotentReplay: true });
    expect(replay.body.balance?.amount).toBe("70.00");
    const rejected = wager(w, "BET", "9999.00");
    const r1 = await env.submit(api.baseUrl, rejected);
    const r2 = await env.submit(api.baseUrl, rejected);
    expect([r1.status, r2.status]).toEqual([422, 422]);
    expect(r2.body.idempotentReplay).toBe(true);
    expect(r2.body.transactionId).toBe(r1.body.transactionId);
  });

  test("full round: BET, WIN, LOSS, REFUND, ROLLBACK, currency conflict", async () => {
    const w = await env.createWallet(api.baseUrl, "100.00");
    const bet = wager(w, "BET", "10.00");
    await env.submit(api.baseUrl, bet);
    const win = wager(w, "WIN", "25.00", { referenceExternalTransactionId: bet.externalTransactionId });
    expect((await env.submit(api.baseUrl, win)).body.balance?.amount).toBe("115.00");
    const loss = await env.submit(api.baseUrl, wager(w, "LOSS", "0.00"));
    expect(loss.body).toMatchObject({ status: "PROCESSED", balance: { amount: "115.00" } });
    const bet2 = wager(w, "BET", "15.00");
    await env.submit(api.baseUrl, bet2);
    const refund = await env.submit(api.baseUrl, wager(w, "REFUND", "15.00", { referenceExternalTransactionId: bet2.externalTransactionId }));
    expect(refund.body.balance?.amount).toBe("115.00");
    const rollback = await env.submit(api.baseUrl, wager(w, "ROLLBACK", "15.00", { referenceExternalTransactionId: bet2.externalTransactionId }));
    expect(rollback.body).toMatchObject({ status: "REJECTED", failureCode: "REFERENCE_ALREADY_REVERSED" });
    const rbWin = await env.submit(api.baseUrl, wager(w, "ROLLBACK", "25.00", { referenceExternalTransactionId: win.externalTransactionId }));
    expect(rbWin.body.balance?.amount).toBe("90.00");
    const usd = await env.submit(api.baseUrl, wager(w, "WIN", "1.00", { money: { amount: "1.00", currency: "USD" } }));
    expect(usd.body).toMatchObject({ status: "REJECTED", failureCode: "CURRENCY_MISMATCH" });

    const wallet = await get(`/wallets/${w.walletId}`);
    expect(wallet.body.balance.amount).toBe("90.00");
    // LOSS and rejections did not bump the version: opening(1) bet win bet2 refund rbWin = 6
    expect(wallet.body.version).toBe(6);
    const byExternal = await get(`/providers/provider-a/wagering/transactions/${bet.externalTransactionId}`);
    expect(byExternal.body.status).toBe("PROCESSED");
  });

  test("reversal that would make the balance negative is rejected explicitly", async () => {
    const w = await env.createWallet(api.baseUrl, "0.00");
    const win = wager(w, "WIN", "50.00");
    await env.submit(api.baseUrl, win);
    await env.submit(api.baseUrl, wager(w, "BET", "45.00"));
    const rb = await env.submit(api.baseUrl, wager(w, "ROLLBACK", "50.00", { referenceExternalTransactionId: win.externalTransactionId }));
    expect(rb.status).toBe(422);
    expect(rb.body.failureCode).toBe("REVERSAL_INSUFFICIENT_FUNDS");
    const tx = await get(`/wagering/transactions/${rb.body.transactionId}`);
    expect(tx.body).toMatchObject({ status: "REJECTED", failureCode: "REVERSAL_INSUFFICIENT_FUNDS" });
  });
});

describe("out-of-order references (HTTP)", () => {
  test("REFUND before its BET is parked, then settled by the worker once the BET arrives", async () => {
    const w = await env.createWallet(api.baseUrl, "100.00");
    const bet = wager(w, "BET", "40.00");
    const refund = wager(w, "REFUND", "40.00", { referenceExternalTransactionId: bet.externalTransactionId });
    const first = await env.submit(api.baseUrl, refund);
    expect(first.status).toBe(202);
    // pending replays stay 202
    expect((await env.submit(api.baseUrl, refund)).status).toBe(202);
    await env.submit(api.baseUrl, bet);
    const settled = await eventually(async () => {
      const tx = await get(`/wagering/transactions/${first.body.transactionId}`);
      if (tx.body.status !== "PROCESSED") throw new Error(tx.body.status);
      return tx.body;
    });
    expect(settled.referenceTransactionId).toBeDefined();
    expect((await get(`/wallets/${w.walletId}`)).body.balance.amount).toBe("100.00");
    const replay = await env.submit(api.baseUrl, refund);
    expect(replay.status).toBe(200);
    expect(replay.body.status).toBe("PROCESSED");
  });

  test("missing reference is rejected REFERENCE_NOT_FOUND after the retry budget, with an event", async () => {
    const w = await env.createWallet(api.baseUrl, "100.00");
    const rb = await env.submit(api.baseUrl, wager(w, "ROLLBACK", "1.00", { referenceExternalTransactionId: "never" }));
    const final = await eventually(
      async () => {
        const tx = await get(`/wagering/transactions/${rb.body.transactionId}`);
        if (tx.body.status !== "REJECTED") throw new Error(tx.body.status);
        return tx.body;
      },
      { timeoutMs: 15_000 },
    );
    expect(final.failureCode).toBe("REFERENCE_NOT_FOUND");
    const events = await env.sql`select event_type from outbox_messages where payload->'data'->>'transactionId' = ${rb.body.transactionId} order by seq`;
    expect(events.map((e: any) => e.event_type)).toEqual(["WagerTransactionPendingReference", "WagerTransactionRejected"]);
  });
});

describe("ledger, reconciliation, health", () => {
  test("ledger pagination uses a stable opaque cursor", async () => {
    const w = await env.createWallet(api.baseUrl, "100.00");
    for (let i = 0; i < 5; i++) await env.submit(api.baseUrl, wager(w, "BET", "1.00"));
    const seen: number[] = [];
    let cursor: string | null = null;
    do {
      const page = await get(`/wallets/${w.walletId}/ledger?limit=2${cursor ? `&cursor=${cursor}` : ""}`);
      seen.push(...page.body.items.map((i: any) => i.walletVersion));
      cursor = page.body.nextCursor;
    } while (cursor);
    expect(seen).toEqual([1, 2, 3, 4, 5, 6]);
    expect((await get(`/wallets/${w.walletId}/ledger?cursor=garbage`)).status).toBe(400);
  });

  test("reconciliation reports consistency and flags divergence without fixing it", async () => {
    const w = await env.createWallet(api.baseUrl, "100.00");
    await env.submit(api.baseUrl, wager(w, "BET", "25.00"));
    const ok = await fetch(`${api.baseUrl}/wallets/${w.walletId}/reconciliation`, { method: "POST" });
    expect(await ok.json()).toEqual({
      walletId: w.walletId,
      storedBalance: { amount: "75.00", currency: "BRL" },
      calculatedBalance: { amount: "75.00", currency: "BRL" },
      difference: { amount: "0.00", currency: "BRL" },
      consistent: true,
      checkedEntries: 2,
    });

    // Simulate corruption by an operator bypassing the guards (session_replication_role disables triggers).
    await env.sql.begin(async (tx) => {
      await tx`set local session_replication_role = replica`;
      await tx`update wallets set balance = 80 where id = ${w.walletId}`;
    });
    const bad = (await (await fetch(`${api.baseUrl}/wallets/${w.walletId}/reconciliation`, { method: "POST" })).json()) as any;
    expect(bad.consistent).toBe(false);
    expect(bad.difference).toEqual({ amount: "5.00", currency: "BRL" });
    expect((await env.walletRow(w.walletId)).balance).toBe("80.00"); // not silently corrected
    const metrics = await (await fetch(`${api.baseUrl}/metrics`)).text();
    expect(metrics).toContain('wagering_reconciliations_total{result="divergent"} 1');
    // restore for the global invariant check
    await env.sql.begin(async (tx) => {
      await tx`set local session_replication_role = replica`;
      await tx`update wallets set balance = 75 where id = ${w.walletId}`;
    });
  });

  test("health endpoints are open; readiness checks Postgres and SQS", async () => {
    expect((await get("/health/live")).body).toEqual({ status: "ok" });
    const ready = await get("/health/ready");
    expect(ready.status).toBe(200);
    expect(ready.body.checks).toEqual({ postgres: { status: "up" }, sqs: { status: "up" } });
  });

  test("metrics expose the required series", async () => {
    const text = await (await fetch(`${api.baseUrl}/metrics`)).text();
    for (const name of [
      "wagering_transactions_total", "wagering_duplicates_total", "wagering_processing_duration_seconds",
      "outbox_lag_seconds", "sqs_dlq_depth", "wagering_pending_reference_transactions",
    ]) expect(text).toContain(name);
  });
});
