import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { type Instance, TestEnv, wager } from "../support/env";

/*
 * Real parallelism: three NestJS instances, each with its own connection pool, hammered with
 * concurrent HTTP requests. Nothing is serialised in the test — the database must do it.
 */
let env: TestEnv;
let apps: Instance[];
const pick = (i: number) => apps[i % apps.length]!.baseUrl;

beforeAll(async () => {
  env = await TestEnv.create("hot");
  const opts = { ENABLE_CONSUMER: "false", ENABLE_OUTBOX_PUBLISHER: "false", DB_POOL_MAX: "20" };
  apps = await Promise.all([env.startInProcess(opts), env.startInProcess(opts), env.startInProcess(opts)]);
});
afterAll(async () => {
  await env.assertGlobalInvariants();
  await env.dispose();
});

describe("concurrency", () => {
  test("1. the same bet sent 50 times in parallel debits exactly once", async () => {
    // Ten wallets at once (500 requests) to widen the race windows (lookup → lock → insert).
    const wallets = await Promise.all(Array.from({ length: 10 }, (_, i) => env.createWallet(pick(i), "100.00")));
    await Promise.all(
      wallets.map(async (w) => {
        const bet = wager(w, "BET", "10.00");
        const results = await Promise.all(Array.from({ length: 50 }, (_, i) => env.submit(pick(i), bet)));
        // identical requests are never a conflict: only 200 (or a retryable 503 under pool pressure)
        expect(results.filter((r) => r.status !== 200 && r.status !== 503).map((r) => [r.status, r.body])).toEqual([]);
        const ok = results.filter((r) => r.status === 200);
        expect(new Set(ok.map((r) => r.body.transactionId)).size).toBe(1);
        expect(ok.filter((r) => !r.body.idempotentReplay).length).toBeLessThanOrEqual(1);
        expect(ok.every((r) => r.body.balance?.amount === "90.00")).toBe(true);
        expect((await env.ledgerRows(w.walletId)).filter((l) => l.direction === "DEBIT")).toHaveLength(1);
        expect(await env.walletRow(w.walletId)).toEqual({ balance: "90.00", version: 2 });
      }),
    );
  });

  test("2. section 8: 100.00 and two simultaneous 80.00 bets → one PROCESSED, one REJECTED, 20.00 left", async () => {
    // Repeated on 25 wallets at once to make the race real and frequent.
    const wallets = await Promise.all(Array.from({ length: 25 }, (_, i) => env.createWallet(pick(i), "100.00")));
    await Promise.all(
      wallets.map(async (w, i) => {
        const [a, b] = await Promise.all([
          env.submit(pick(i), wager(w, "BET", "80.00")),
          env.submit(pick(i + 1), wager(w, "BET", "80.00")),
        ]);
        const statuses = [a.body.status, b.body.status].sort();
        expect(statuses).toEqual(["PROCESSED", "REJECTED"]);
        const rejected = a.body.status === "REJECTED" ? a : b;
        expect(rejected.status).toBe(422);
        expect(rejected.body.failureCode).toBe("INSUFFICIENT_FUNDS");
      }),
    );
    for (const w of wallets) {
      expect(await env.walletRow(w.walletId)).toEqual({ balance: "20.00", version: 2 });
      const debits = (await env.ledgerRows(w.walletId)).filter((l) => l.direction === "DEBIT");
      expect(debits).toHaveLength(1);
      expect(debits[0]!.amount).toBe("80.00");
    }
    // retries of both bets never duplicate the debit
    const w = wallets[0]!;
    const txs = await env.transactionsOf(w.walletId);
    await Promise.all(
      txs.flatMap((t) =>
        Array.from({ length: 10 }, (_, i) =>
          env.submit(pick(i), { ...wager(w, "BET", "80.00"), externalTransactionId: t.external_transaction_id }),
        ),
      ),
    );
    expect((await env.ledgerRows(w.walletId)).filter((l) => l.direction === "DEBIT")).toHaveLength(1);
  });

  test("hot wallet: 200 concurrent mixed operations keep balance == ledger and never go negative", async () => {
    const w = await env.createWallet(pick(0), "50.00");
    const ops = Array.from({ length: 200 }, (_, i) =>
      i % 3 === 0 ? wager(w, "WIN", "3.00") : wager(w, "BET", "2.50"),
    );
    // Like a provider: a 503 (transient, e.g. pool exhausted behind the hot-wallet lock) is retried
    // with the same Idempotency-Key until a terminal answer comes back.
    let transient = 0;
    const results = await Promise.all(
      ops.map(async (op, i) => {
        for (let attempt = 0; ; attempt++) {
          const r = await env.submit(pick(i + attempt), op);
          if (r.status !== 503 || attempt > 20) return r;
          transient++;
          expect(r.body.error?.code).toBe("TRANSIENT_FAILURE");
          await new Promise((res) => setTimeout(res, 50 * (attempt + 1)));
        }
      }),
    );
    expect(results.filter((r) => ![200, 422].includes(r.status)).map((r) => [r.status, r.body])).toEqual([]);
    const processed = results.filter((r) => r.body.status === "PROCESSED").length;
    const ledger = await env.ledgerRows(w.walletId);
    expect(ledger).toHaveLength(processed + 1);
    expect(ledger.map((l) => l.version)).toEqual(ledger.map((_, i) => i + 1));
    const last = ledger.at(-1)!;
    expect((await env.walletRow(w.walletId)).balance).toBe(last.after);
    // every operation exists exactly once, whatever the number of retries
    expect(await env.transactionsOf(w.walletId)).toHaveLength(200);
    console.log(`hot wallet: ${processed} processed, ${200 - processed} rejected, ${transient} transient 503s retried`);
  });

  test("3. distinct wallets are processed in parallel (no global lock)", async () => {
    const wallets = await Promise.all(Array.from({ length: 20 }, (_, i) => env.createWallet(pick(i), "100.00")));
    const results = await Promise.all(
      wallets.flatMap((w, i) => Array.from({ length: 10 }, (_, j) => env.submit(pick(i + j), wager(w, "BET", "5.00")))),
    );
    expect(results.every((r) => r.status === 200)).toBe(true);
    for (const w of wallets) expect(await env.walletRow(w.walletId)).toEqual({ balance: "50.00", version: 11 });

    // Proof of per-wallet locking: while wallet A is locked by another session, wallet B still
    // settles immediately, and A's request completes as soon as the lock is released.
    const [a, b] = wallets;
    const holder = await env.sql.reserve();
    await holder`begin`;
    await holder`select id from wallets where id = ${a!.walletId} for update`;
    let aDone = false;
    const aRequest = env.submit(pick(0), wager(a!, "BET", "1.00")).then((r) => {
      aDone = true;
      return r;
    });
    const t0 = performance.now();
    const bResult = await env.submit(pick(1), wager(b!, "BET", "1.00"));
    const bElapsed = performance.now() - t0;
    expect(bResult.status).toBe(200);
    expect(bElapsed).toBeLessThan(1_000);
    expect(aDone).toBe(false);
    await holder`rollback`;
    holder.release();
    expect((await aRequest).status).toBe(200);
  });
});
