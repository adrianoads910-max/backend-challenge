import { afterEach, describe, expect, test } from "bun:test";
import { eventually, TestEnv, wager } from "../support/env";

/* The events queue is a STANDARD queue here: no broker dedup, so publisher duplicates are visible. */
let env: TestEnv | undefined;
afterEach(async () => {
  if (env) {
    await env.assertGlobalInvariants();
    await env.dispose();
    env = undefined;
  }
});

async function allUnpublished(e: TestEnv) {
  const [r] = await e.sql`select count(*)::int as n from outbox_messages where published_at is null`;
  return r.n as number;
}

/** Reads the events queue until no new message shows up for a while. */
async function collectEvents(e: TestEnv, expected: number) {
  const seen: { eventId: string; body: string }[] = [];
  const deadline = Date.now() + 60_000;
  while (Date.now() < deadline) {
    const batch = await e.receiveAll(e.queues.events, { waitMs: 1_500, visibility: 600 });
    for (const m of batch) seen.push({ eventId: JSON.parse(m.body).eventId, body: m.body });
    if (batch.length === 0 && seen.length >= expected) break;
  }
  return seen;
}

async function outboxIds(e: TestEnv): Promise<Set<string>> {
  return new Set<string>((await e.sql`select id::text from outbox_messages`).map((r: any) => r.id as string));
}

describe("6. outbox publishers", () => {
  test("two concurrent publishers over the same outbox publish every event exactly once", async () => {
    env = await TestEnv.create("pubs", { standardEvents: true });
    const api = await env.startInProcess({ ENABLE_CONSUMER: "false", ENABLE_OUTBOX_PUBLISHER: "false" });
    // two real processes competing for the same rows
    await Promise.all([
      env.spawn({ ENABLE_CONSUMER: "false", ENABLE_PENDING_REFERENCE_WORKER: "false", OUTBOX_BATCH_SIZE: "7" }),
      env.spawn({ ENABLE_CONSUMER: "false", ENABLE_PENDING_REFERENCE_WORKER: "false", OUTBOX_BATCH_SIZE: "7" }),
    ]);
    const wallets = await Promise.all(Array.from({ length: 10 }, () => env!.createWallet(api.baseUrl, "100.00")));
    await Promise.all(wallets.flatMap((w) => Array.from({ length: 10 }, () => env!.submit(api.baseUrl, wager(w, "BET", "1.00")))));
    await eventually(async () => {
      if ((await allUnpublished(env!)) > 0) throw new Error("pending");
    }, { timeoutMs: 30_000 });

    const ids = await outboxIds(env);
    const events = await collectEvents(env, ids.size);
    const counts = new Map<string, number>();
    for (const e of events) counts.set(e.eventId, (counts.get(e.eventId) ?? 0) + 1);
    expect(new Set(counts.keys())).toEqual(ids);
    expect([...counts.values()].filter((n) => n > 1)).toEqual([]); // no duplicates without crashes
    const publishers = await env.sql`select count(distinct locked_by)::int as n from outbox_messages`;
    expect(publishers[0].n).toBe(0); // leases released after marking
  });

  test("process dies after commit and before publishing: another instance publishes", async () => {
    env = await TestEnv.create("prepub", { standardEvents: true });
    const writer = await env.spawn({ ENABLE_OUTBOX_PUBLISHER: "false", ENABLE_CONSUMER: "false" });
    const w = await env.createWallet(writer.baseUrl, "100.00");
    await env.submit(writer.baseUrl, wager(w, "BET", "10.00"));
    writer.kill("SIGKILL");
    await writer.exited;
    const committed = await outboxIds(env);
    expect(await allUnpublished(env)).toBe(committed.size);

    await env.spawn({ ENABLE_CONSUMER: "false" });
    await eventually(async () => {
      if ((await allUnpublished(env!)) > 0) throw new Error("pending");
    });
    const events = await collectEvents(env, committed.size);
    expect(new Set(events.map((e) => e.eventId))).toEqual(committed);
  });

  test("process dies after publishing and before marking: republished, duplicates are identical and dedupable", async () => {
    env = await TestEnv.create("postpub", { standardEvents: true });
    const api = await env.startInProcess({ ENABLE_CONSUMER: "false", ENABLE_OUTBOX_PUBLISHER: "false" });
    const w = await env.createWallet(api.baseUrl, "100.00");
    for (let i = 0; i < 5; i++) await env.submit(api.baseUrl, wager(w, "BET", "1.00"));
    const committed = await outboxIds(env);

    const doomed = await env.spawn(
      { ENABLE_CONSUMER: "false", FAULT_INJECTION: "crash_after_publish_before_mark", OUTBOX_LEASE_MS: "2000" },
      { waitReady: false },
    );
    await doomed.exited;
    expect(doomed.logs()).toContain("fault injection: crashing");
    expect(await allUnpublished(env)).toBe(committed.size); // published to SQS but not marked

    await env.spawn({ ENABLE_CONSUMER: "false", OUTBOX_LEASE_MS: "2000" });
    await eventually(async () => {
      if ((await allUnpublished(env!)) > 0) throw new Error("pending");
    }, { timeoutMs: 20_000 });

    const events = await collectEvents(env, committed.size * 2);
    const byId = new Map<string, Set<string>>();
    for (const e of events) byId.set(e.eventId, (byId.get(e.eventId) ?? new Set()).add(e.body));
    expect(new Set(byId.keys())).toEqual(committed); // nothing lost
    expect(events.length).toBeGreaterThan(committed.size); // at-least-once: duplicates happened…
    expect([...byId.values()].every((bodies) => bodies.size === 1)).toBe(true); // …and are byte-identical
  });
});

describe("7. references delivered before the referenced transaction", () => {
  test("REFUND and ROLLBACK arriving first over the queue settle once their references arrive", async () => {
    env = await TestEnv.create("ooo");
    await Promise.all([env.spawn(), env.spawn(), env.spawn()]);
    const api = await env.startInProcess({ ENABLE_CONSUMER: "false" });
    const wallets = await Promise.all(Array.from({ length: 8 }, () => env!.createWallet(api.baseUrl, "100.00")));

    const plan = wallets.map((w) => {
      const bet = wager(w, "BET", "20.00");
      const win = wager(w, "WIN", "50.00");
      return {
        w,
        bet,
        win,
        refund: wager(w, "REFUND", "20.00", { referenceExternalTransactionId: bet.externalTransactionId }),
        rollback: wager(w, "ROLLBACK", "50.00", { referenceExternalTransactionId: win.externalTransactionId }),
      };
    });
    // reversals first
    for (const p of plan) {
      await env.sendWagerMessage(p.refund);
      await env.sendWagerMessage(p.rollback);
    }
    await eventually(async () => {
      const [r] = await env!.sql`select count(*)::int as n from wager_transactions where status = 'PENDING_REFERENCE'`;
      if (r.n < plan.length * 2) throw new Error(`${r.n}`);
    });
    // then the references
    for (const p of plan) {
      await env.sendWagerMessage(p.bet);
      await env.sendWagerMessage(p.win);
    }
    await eventually(async () => {
      const rows = await env!.sql`select status from wager_transactions where kind in ('REFUND','ROLLBACK') and status <> 'PROCESSED'`;
      if (rows.length > 0) throw new Error(`${rows.length} not processed`);
    }, { timeoutMs: 30_000 });

    for (const p of plan) {
      expect((await env.walletRow(p.w.walletId)).balance).toBe("100.00");
      const txs = await env.transactionsOf(p.w.walletId);
      expect(txs.map((t) => `${t.kind}:${t.status}`).sort()).toEqual([
        "BET:PROCESSED", "REFUND:PROCESSED", "ROLLBACK:PROCESSED", "WIN:PROCESSED",
      ]);
    }
    const events = await env.sql`select count(*)::int as n from outbox_messages where event_type = 'WagerTransactionPendingReference'`;
    expect(events[0].n).toBe(plan.length * 2);
  });
});
