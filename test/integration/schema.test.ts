import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import type { SQL } from "bun";
import { migrate } from "../../src/migrate";
import { TestEnv } from "../support/env";

/** Invariants enforced by the schema itself — raw SQL, no application code involved. */
let env: TestEnv;
let sql: SQL;

beforeAll(async () => {
  env = await TestEnv.create("schema");
  sql = env.sql;
});
afterAll(async () => {
  await env.dispose();
});

const uuid = () => crypto.randomUUID();
const now = new Date().toISOString();

async function rejects(fn: () => Promise<unknown>, pattern: RegExp) {
  let error: unknown;
  try {
    await fn();
  } catch (err) {
    error = err;
  }
  expect(error).toBeDefined();
  expect(String((error as Error).message)).toMatch(pattern);
}

/** Opens a wallet the way the application does: wallet + OPENING tx + opening ledger, one transaction. */
async function openWallet(balance = "100.00", playerId = uuid()) {
  const walletId = uuid();
  const txId = uuid();
  await sql.begin(async (tx) => {
    await tx`insert into wallets values (${walletId}, ${playerId}, 'BRL', ${balance}, 1, ${now}, ${now})`;
    if (balance !== "0.00") {
      await tx`insert into wager_transactions (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id,
                 player_id, round_id, game_id, kind, amount, currency, status, result_balance, processed_at, created_at, updated_at)
               values (${txId}, '__internal__', ${"opening:" + walletId}, ${"__internal__:opening:" + walletId}, ${"0".repeat(64)},
                 ${walletId}, ${playerId}, 'opening', 'opening', 'OPENING', ${balance}, 'BRL', 'PROCESSED', ${balance}, ${now}, ${now}, ${now})`;
      await tx`insert into wallet_ledger_entries values (${uuid()}, ${walletId}, 'BRL', ${txId}, 1, 'CREDIT', ${balance}, 0, ${balance}, ${now})`;
    }
  });
  return { walletId, playerId, openingTxId: txId };
}

async function insertTx(walletId: string, playerId: string, over: Record<string, unknown> = {}) {
  const row = {
    id: uuid(), provider_id: "prov", external_transaction_id: uuid(), idempotency_key: uuid(), payload_hash: "a".repeat(64),
    wallet_id: walletId, player_id: playerId, round_id: "r", game_id: "g", kind: "BET", amount: "10.00", currency: "BRL",
    status: "PROCESSED", processed_at: now, created_at: now, updated_at: now, ...over,
  };
  await sql`insert into wager_transactions ${sql(row)}`;
  return row.id as string;
}

describe("migrations", () => {
  test("are reversible (down then up)", async () => {
    const other = await TestEnv.create("schema_rev");
    try {
      await migrate("down", other.databaseUrl);
      const [{ n }] = await other.sql`select count(*)::int as n from information_schema.tables where table_name = 'wallets'`;
      expect(n).toBe(0);
      await migrate("up", other.databaseUrl);
      const [{ m }] = await other.sql`select count(*)::int as m from information_schema.tables where table_name in ('wallets','wager_transactions','wallet_ledger_entries','inbox_messages','outbox_messages')`;
      expect(m).toBe(5);
    } finally {
      await other.dispose();
    }
  });
});

describe("wallet constraints", () => {
  test("one wallet per player and currency", async () => {
    const { playerId } = await openWallet();
    await rejects(() => openWallet("0.00", playerId), /wallets_player_currency_uq/);
  });

  test("balance cannot be negative", async () => {
    await rejects(() => sql`insert into wallets values (${uuid()}, ${uuid()}, 'BRL', -1, 1, ${now}, ${now})`, /wallets_balance_non_negative/);
  });

  test("balance cannot change without a matching ledger entry (checked at commit)", async () => {
    const { walletId } = await openWallet();
    await rejects(() => sql`update wallets set balance = 50, version = 2 where id = ${walletId}`, /diverges from ledger/);
  });

  test("version only moves with the balance, by exactly one", async () => {
    const { walletId } = await openWallet();
    await rejects(() => sql`update wallets set version = 5 where id = ${walletId}`, /version changed without balance change/);
    await rejects(() => sql`update wallets set balance = 50, version = 3 where id = ${walletId}`, /without version increment/);
  });

  test("a wallet created with balance must have its opening entry", async () => {
    await rejects(() => sql`insert into wallets values (${uuid()}, ${uuid()}, 'BRL', 10, 1, ${now}, ${now})`, /no ledger entries/);
  });

  test("wallets cannot be deleted", async () => {
    const { walletId } = await openWallet();
    await rejects(() => sql`delete from wallets where id = ${walletId}`, /append-only/);
  });
});

describe("ledger constraints", () => {
  test("a consistent debit (tx + ledger + wallet in one transaction) is accepted", async () => {
    const { walletId, playerId } = await openWallet();
    await sql.begin(async (tx) => {
      const txId = uuid();
      await tx`insert into wager_transactions (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id, round_id, game_id, kind, amount, currency, status, processed_at, created_at, updated_at)
               values (${txId}, 'prov', ${uuid()}, ${uuid()}, ${"a".repeat(64)}, ${walletId}, ${playerId}, 'r', 'g', 'BET', 30, 'BRL', 'PROCESSED', ${now}, ${now}, ${now})`;
      await tx`insert into wallet_ledger_entries values (${uuid()}, ${walletId}, 'BRL', ${txId}, 2, 'DEBIT', 30, 100, 70, ${now})`;
      await tx`update wallets set balance = 70, version = 2 where id = ${walletId}`;
    });
    expect((await env.walletRow(walletId)).balance).toBe("70.00");
  });

  test("is append-only: no UPDATE, DELETE or TRUNCATE", async () => {
    const { walletId } = await openWallet();
    await rejects(() => sql`update wallet_ledger_entries set amount = 1 where wallet_id = ${walletId}`, /append-only/);
    await rejects(() => sql`delete from wallet_ledger_entries where wallet_id = ${walletId}`, /append-only/);
    await rejects(() => sql`truncate wallet_ledger_entries cascade`, /append-only/);
  });

  test("entry arithmetic is checked", async () => {
    const { walletId, playerId } = await openWallet();
    const txId = await insertTx(walletId, playerId);
    await rejects(
      () => sql`insert into wallet_ledger_entries values (${uuid()}, ${walletId}, 'BRL', ${txId}, 2, 'DEBIT', 10, 100, 91, ${now})`,
      /ledger_entry_balanced/,
    );
  });

  test("entry must chain from the previous one", async () => {
    const { walletId, playerId } = await openWallet();
    await rejects(
      () =>
        sql.begin(async (tx) => {
          const txId = uuid();
          await tx`insert into wager_transactions (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id, round_id, game_id, kind, amount, currency, status, processed_at, created_at, updated_at)
                   values (${txId}, 'prov', ${uuid()}, ${uuid()}, ${"a".repeat(64)}, ${walletId}, ${playerId}, 'r', 'g', 'BET', 10, 'BRL', 'PROCESSED', ${now}, ${now}, ${now})`;
          await tx`insert into wallet_ledger_entries values (${uuid()}, ${walletId}, 'BRL', ${txId}, 2, 'DEBIT', 10, 50, 40, ${now})`;
          await tx`update wallets set balance = 40, version = 2 where id = ${walletId}`;
        }),
      /does not chain/,
    );
  });

  test("at most one entry per (transaction, wallet) and per wallet version", async () => {
    const { walletId, openingTxId } = await openWallet();
    await rejects(
      () => sql`insert into wallet_ledger_entries values (${uuid()}, ${walletId}, 'BRL', ${openingTxId}, 2, 'CREDIT', 1, 100, 101, ${now})`,
      /ledger_one_entry_per_tx_wallet_uq/,
    );
  });

  test("entries must be in the wallet currency", async () => {
    const { walletId, playerId } = await openWallet();
    const txId = await insertTx(walletId, playerId, { kind: "WIN", currency: "USD", amount: "1.00" });
    await rejects(
      () => sql`insert into wallet_ledger_entries values (${uuid()}, ${walletId}, 'USD', ${txId}, 2, 'CREDIT', 1, 100, 101, ${now})`,
      /ledger_wallet_currency_fk/,
    );
  });

  test("LOSS or non-processed transactions cannot have ledger entries", async () => {
    const { walletId, playerId } = await openWallet();
    await rejects(
      () =>
        sql.begin(async (tx) => {
          const txId = uuid();
          await tx`insert into wager_transactions (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id, round_id, game_id, kind, amount, currency, status, failure_code, processed_at, created_at, updated_at)
                   values (${txId}, 'prov', ${uuid()}, ${uuid()}, ${"a".repeat(64)}, ${walletId}, ${playerId}, 'r', 'g', 'BET', 10, 'BRL', 'REJECTED', 'INSUFFICIENT_FUNDS', ${now}, ${now}, ${now})`;
          await tx`insert into wallet_ledger_entries values (${uuid()}, ${walletId}, 'BRL', ${txId}, 2, 'DEBIT', 10, 100, 90, ${now})`;
          await tx`update wallets set balance = 90, version = 2 where id = ${walletId}`;
        }),
      /does not match a processed transaction/,
    );
  });
});

describe("wager transaction constraints", () => {
  test("idempotency key and (provider, external id) are unique", async () => {
    const { walletId, playerId } = await openWallet();
    await insertTx(walletId, playerId, { idempotency_key: "dup-key", status: "PROCESSED", kind: "LOSS" });
    await rejects(() => insertTx(walletId, playerId, { idempotency_key: "dup-key", kind: "LOSS" }), /wager_tx_idempotency_key_uq/);
    await insertTx(walletId, playerId, { external_transaction_id: "dup-ext", kind: "LOSS" });
    await rejects(() => insertTx(walletId, playerId, { external_transaction_id: "dup-ext", kind: "LOSS" }), /wager_tx_provider_external_id_uq/);
  });

  test("terminal transactions are immutable", async () => {
    const { walletId, playerId } = await openWallet();
    const id = await insertTx(walletId, playerId, { kind: "LOSS" });
    await rejects(() => sql`update wager_transactions set status = 'REJECTED', failure_code = 'X' where id = ${id}`, /is terminal/);
    await rejects(() => sql`delete from wager_transactions where id = ${id}`, /append-only/);
  });

  test("business columns never change, even before a terminal state", async () => {
    const { walletId, playerId } = await openWallet();
    const id = await insertTx(walletId, playerId, {
      kind: "REFUND", status: "PENDING_REFERENCE", processed_at: null, next_attempt_at: now, reference_external_transaction_id: "x",
    });
    await rejects(() => sql`update wager_transactions set amount = 99 where id = ${id}`, /immutable/);
  });

  test("REFUND/ROLLBACK require a reference; rejections require a failure code; OPENING is internal", async () => {
    const { walletId, playerId } = await openWallet();
    await rejects(() => insertTx(walletId, playerId, { kind: "REFUND" }), /wager_tx_reversal_requires_reference/);
    await rejects(() => insertTx(walletId, playerId, { status: "REJECTED" }), /wager_tx_failure_code_iff_failed/);
    await rejects(() => insertTx(walletId, playerId, { kind: "OPENING" }), /wager_tx_opening_is_internal/);
    await rejects(() => insertTx(walletId, playerId, { amount: "-1.00" }), /wager_tx_amount_non_negative/);
  });

  test("a reference can be successfully reversed only once", async () => {
    const { walletId, playerId, openingTxId } = await openWallet();
    const base = { kind: "REFUND", status: "PROCESSED", reference_external_transaction_id: "b", reference_transaction_id: openingTxId };
    await insertTx(walletId, playerId, base);
    await rejects(() => insertTx(walletId, playerId, { ...base, kind: "ROLLBACK" }), /wager_tx_single_reversal_uq/);
    // a REJECTED attempt does not count
    await insertTx(walletId, playerId, { ...base, status: "REJECTED", failure_code: "REFERENCE_ALREADY_REVERSED" });
  });
});

describe("inbox", () => {
  test("(consumer, message id) is unique", async () => {
    await sql`insert into inbox_messages values ('c', 'm', ${"a".repeat(64)}, ${now}, ${now})`;
    await rejects(() => sql`insert into inbox_messages values ('c', 'm', ${"b".repeat(64)}, ${now}, ${now})`, /inbox_messages_pkey/);
    await sql`insert into inbox_messages values ('other', 'm', ${"a".repeat(64)}, ${now}, ${now})`;
  });
});
