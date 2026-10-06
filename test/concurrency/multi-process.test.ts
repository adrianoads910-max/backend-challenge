import { afterEach, describe, expect, test } from "bun:test";
import { eventually, sleep, type SpawnedInstance, TestEnv, wager, type WagerPayload } from "../support/env";

/*
 * Separate OS processes (`bun src/main.ts`), each with HTTP + consumer + outbox publisher +
 * pending-reference worker enabled — the production topology.
 */
let env: TestEnv | undefined;
afterEach(async () => {
  if (env) {
    await env.assertGlobalInvariants();
    await env.dispose();
    env = undefined;
  }
});

async function untilQueueEmpty(e: TestEnv, timeoutMs = 60_000) {
  await eventually(async () => {
    const d = await e.queueDepth(e.queues.wager);
    if (d.visible + d.inFlight > 0) throw new Error(JSON.stringify(d));
  }, { timeoutMs, intervalMs: 300, message: "wager queue to drain" });
}

async function countByExternal(e: TestEnv, ids: string[]) {
  const rows = await e.sql`select external_transaction_id, count(*)::int as n from wager_transactions
                             where external_transaction_id in ${e.sql(ids)} group by 1`;
  return new Map(rows.map((r: any) => [r.external_transaction_id, r.n]));
}

describe("multiple processes", () => {
  test("4. three processes share HTTP and queue traffic on the same wallets and stay consistent", async () => {
    env = await TestEnv.create("mp3");
    const procs = await Promise.all([env.spawn(), env.spawn(), env.spawn()]);
    const wallets = await Promise.all(Array.from({ length: 10 }, (_, i) => env!.createWallet(procs[i % 3]!.baseUrl, "100.00")));

    const ops: WagerPayload[] = wallets.flatMap((w) => [
      ...Array.from({ length: 6 }, () => wager(w, "BET", "7.00")),
      ...Array.from({ length: 4 }, () => wager(w, "WIN", "3.00")),
    ]);
    // every op is sent twice: once over HTTP (random process), once over SQS (any consumer)
    await Promise.all(ops.map((op) => env!.sendWagerMessage(op)));
    const http = await Promise.all(ops.map((op, i) => env!.submit(procs[i % 3]!.baseUrl, op)));
    expect(http.every((r) => r.status === 200 || r.status === 422 || r.status === 503)).toBe(true);
    await untilQueueEmpty(env);

    const counts = await countByExternal(env, ops.map((o) => o.externalTransactionId));
    expect([...counts.values()].every((n) => n === 1)).toBe(true);
    expect(counts.size).toBe(ops.length);
    for (const w of wallets) {
      const ledger = await env.ledgerRows(w.walletId);
      const debits = ledger.filter((l) => l.direction === "DEBIT").length;
      const credits = ledger.filter((l) => l.direction === "CREDIT").length - 1;
      expect(debits).toBe(6);
      expect(credits).toBe(4);
      expect((await env.walletRow(w.walletId)).balance).toBe("70.00");
    }
  });

  test("5. worker killed after commit and before ack: redelivery is absorbed by the inbox", async () => {
    env = await TestEnv.create("crashack");
    const api = await env.spawn({ ENABLE_CONSUMER: "false" });
    const w = await env.createWallet(api.baseUrl, "100.00");

    const doomed = await env.spawn({ FAULT_INJECTION: "crash_after_commit_before_ack", CONSUMER_VISIBILITY_TIMEOUT_S: "3" });
    const bet = wager(w, "BET", "30.00");
    const messageId = await env.sendWagerMessage(bet);
    const exitCode = await Promise.race([doomed.exited, sleep(20_000).then(() => "timeout")]);
    expect(exitCode).not.toBe("timeout");
    expect(doomed.logs()).toContain("fault injection: crashing");

    // committed but not acked
    const [tx] = await env.sql`select status from wager_transactions where external_transaction_id = ${bet.externalTransactionId}`;
    expect(tx.status).toBe("PROCESSED");
    expect(await env.sql`select 1 from inbox_messages where message_id = ${messageId}`).toHaveLength(1);
    const depth = await env.queueDepth(env.queues.wager);
    expect(depth.visible + depth.inFlight).toBe(1);

    const survivor = await env.spawn();
    await untilQueueEmpty(env);
    expect(survivor.logs()).toContain("duplicate message ignored (inbox)");
    expect((await env.ledgerRows(w.walletId)).filter((l) => l.direction === "DEBIT")).toHaveLength(1);
    expect(await env.walletRow(w.walletId)).toEqual({ balance: "70.00", version: 2 });
  });

  test("8. restart: SIGKILL mid-burst, a new process finishes the work and the end state is consistent", async () => {
    env = await TestEnv.create("restart");
    const first = await env.spawn();
    const wallets = await Promise.all(Array.from({ length: 10 }, () => env!.createWallet(first.baseUrl, "1000.00")));
    const ops = wallets.flatMap((w) => Array.from({ length: 15 }, (_, i) => wager(w, i % 5 === 0 ? "WIN" : "BET", "10.00")));
    await Promise.all(ops.map((op) => env!.sendWagerMessage(op)));

    await eventually(async () => {
      const [r] = await env!.sql`select count(*)::int as n from wager_transactions where kind <> 'OPENING'`;
      if (r.n < 20) throw new Error(`${r.n}`);
    });
    first.kill("SIGKILL");
    await first.exited;
    const [mid] = await env.sql`select count(*)::int as n from wager_transactions where kind <> 'OPENING'`;
    expect(mid.n).toBeLessThan(ops.length); // killed mid-way

    await env.assertGlobalInvariants(); // consistent even right after the crash
    const second = await env.spawn({ CONSUMER_VISIBILITY_TIMEOUT_S: "3" });
    await untilQueueEmpty(env, 90_000);
    const counts = await countByExternal(env, ops.map((o) => o.externalTransactionId));
    expect(counts.size).toBe(ops.length);
    expect([...counts.values()].every((n) => n === 1)).toBe(true);
    for (const w of wallets) expect((await env.walletRow(w.walletId)).balance).toBe("910.00"); // 1000 - 12*10 + 3*10

    // outbox: every committed event eventually published by the surviving process
    await eventually(async () => {
      const [r] = await env!.sql`select count(*)::int as n from outbox_messages where published_at is null`;
      if (r.n > 0) throw new Error(`${r.n} unpublished`);
    }, { timeoutMs: 30_000 });
    expect(second.logs()).not.toContain('"level":"fatal"');
  });

  test("graceful shutdown: SIGTERM finishes in-flight work and exits cleanly", async () => {
    env = await TestEnv.create("sigterm");
    const proc = await env.spawn();
    const w = await env.createWallet(proc.baseUrl, "1000.00");
    const ops = Array.from({ length: 40 }, () => wager(w, "BET", "1.00"));
    await Promise.all(ops.map((op) => env!.sendWagerMessage(op)));
    await eventually(async () => {
      const [r] = await env!.sql`select count(*)::int as n from wager_transactions where kind = 'BET'`;
      if (r.n < 3) throw new Error(`${r.n}`);
    });
    proc.kill("SIGTERM");
    // NestJS re-raises the signal after the shutdown hooks complete: 143 = 128 + SIGTERM.
    expect([0, 143]).toContain(await proc.exited);
    expect(proc.logs()).toContain("shutdown complete");
    const next = await env.spawn();
    await untilQueueEmpty(env);
    expect((await countByExternal(env, ops.map((o) => o.externalTransactionId))).size).toBe(40);
    expect((await env.walletRow(w.walletId)).balance).toBe("960.00");
    await next.stop();
  });
});
