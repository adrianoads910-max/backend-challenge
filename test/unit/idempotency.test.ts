import { beforeEach, describe, expect, test } from "bun:test";
import { IdempotencyConflictError, InboxConflictError, ValidationError } from "../../src/application/errors";
import { canonicalJson, sha256Canonical } from "../../src/application/payload-hash";
import {
  ProcessWagerTransaction,
  type SubmitWagerTransactionCommand,
  wagerPayloadHash,
} from "../../src/application/process-wager-transaction";
import type { TransactionalRepositories, UnitOfWork } from "../../src/application/ports";
import type { InboxMessage } from "../../src/domain/inbox-message";
import type { WalletLedgerEntry } from "../../src/domain/ledger-entry";
import { Money } from "../../src/domain/money";
import type { OutboxMessage } from "../../src/domain/outbox-message";
import { Wallet } from "../../src/domain/wallet";
import { type WagerTransaction, WagerTransactionKind } from "../../src/domain/wager-transaction";

/**
 * In-memory adapters — unit scope only (the integration suites run against real PostgreSQL).
 * Stores are snapshotted per unit of work and restored on throw, mimicking rollback.
 */
class Memory implements UnitOfWork {
  wallets = new Map<string, Wallet>();
  txs: WagerTransaction[] = [];
  ledger: WalletLedgerEntry[] = [];
  inbox: InboxMessage[] = [];
  outbox: OutboxMessage[] = [];

  async run<T>(work: (r: TransactionalRepositories) => Promise<T>): Promise<T> {
    const snapshot = { txs: [...this.txs], ledger: [...this.ledger], inbox: [...this.inbox], outbox: [...this.outbox] };
    try {
      return await work(this.repos());
    } catch (err) {
      Object.assign(this, snapshot);
      throw err;
    }
  }

  private repos(): TransactionalRepositories {
    return {
      wallets: {
        findById: async (id) => this.wallets.get(id),
        lockForUpdate: async (id) => ({ wallet: this.wallets.get(id), contended: false }),
        lockForShare: async (id) => this.wallets.get(id),
        insert: async (w) => void this.wallets.set(w.id, w),
        saveBalance: async (w) => void this.wallets.set(w.id, w),
      },
      transactions: {
        findById: async (id) => this.txs.find((t) => t.id === id),
        findByIdempotencyKey: async (k) => this.txs.find((t) => t.idempotencyKey === k),
        findByExternalId: async (p, e) => this.txs.find((t) => t.providerId === p && t.externalTransactionId === e),
        hasProcessedReversal: async () => false,
        insert: async (t) => void this.txs.push(t),
        updateState: async () => undefined,
        wakeDependents: async () => 0,
        findDuePendingReferenceIds: async () => [],
      },
      ledger: {
        insert: async (e) => void this.ledger.push(e),
        page: async () => [],
        summarize: async () => {
          throw new Error("unused");
        },
      },
      inbox: {
        receive: async (m) => {
          const existing = this.inbox.find((i) => i.consumerName === m.consumerName && i.messageId === m.messageId);
          if (existing) return { duplicate: true, existing };
          this.inbox.push(m);
          return { duplicate: false };
        },
      },
      outbox: { add: async (ms) => void this.outbox.push(...ms) },
    };
  }
}

const noop = () => undefined;
const metrics = {
  transactionFinished: noop, duplicateDetected: noop, retry: noop, lockConflict: noop, processingLatency: noop, reconciliation: noop,
};
const logger = { info: noop, warn: noop, error: noop, debug: noop };

let mem: Memory;
let useCase: ProcessWagerTransaction;
let walletId: string;

const command = (over: Partial<SubmitWagerTransactionCommand> = {}): SubmitWagerTransactionCommand => ({
  idempotencyKey: "prov:tx-1",
  providerId: "prov",
  externalTransactionId: "tx-1",
  playerId: "p1",
  walletId,
  roundId: "r1",
  gameId: "g",
  kind: WagerTransactionKind.Bet,
  money: { amount: "25.00", currency: "BRL" },
  ...over,
});
const http = { source: "http" as const, correlationId: "c" };

beforeEach(() => {
  mem = new Memory();
  walletId = crypto.randomUUID();
  const now = new Date();
  mem.wallets.set(walletId, Wallet.rehydrate({
    id: walletId, playerId: "p1", currency: "BRL", balance: Money.from({ amount: "100.00", currency: "BRL" }), version: 1, createdAt: now, updatedAt: now,
  }));
  useCase = new ProcessWagerTransaction(
    mem, { now: () => new Date() }, { next: () => crypto.randomUUID() }, metrics, logger,
    { maxAttempts: 3, baseDelayMs: 10, maxDelayMs: 100 },
  );
});

describe("payload hash", () => {
  test("canonical JSON sorts keys recursively and drops undefined", () => {
    expect(canonicalJson({ b: "x", a: { d: "1", c: "2" }, z: undefined })).toBe('{"a":{"c":"2","d":"1"},"b":"x"}');
    expect(sha256Canonical({ a: "1", b: "2" })).toBe(sha256Canonical({ b: "2", a: "1" }));
  });

  test("normalised amounts hash identically; any business field change changes the hash", () => {
    const money25 = Money.from({ amount: "25", currency: "BRL" });
    const base = wagerPayloadHash(command(), money25);
    expect(wagerPayloadHash(command({ money: { amount: "25.00", currency: "BRL" } }), Money.from({ amount: "25.00", currency: "BRL" }))).toBe(base);
    expect(wagerPayloadHash(command({ roundId: "r2" }), money25)).not.toBe(base);
    expect(wagerPayloadHash(command({ kind: WagerTransactionKind.Win }), money25)).not.toBe(base);
    // the idempotency key (transport header) is not part of the hash
    expect(wagerPayloadHash(command({ idempotencyKey: "other" }), money25)).toBe(base);
  });

  test("numbers are refused in hashed payloads", () => {
    expect(() => canonicalJson({ amount: 25 })).toThrow(TypeError);
  });
});

describe("ProcessWagerTransaction idempotency", () => {
  test("identical request is replayed with the original result", async () => {
    const first = await useCase.execute(command(), http);
    const second = await useCase.execute(command(), http);
    expect(first.idempotentReplay).toBe(false);
    expect(second.idempotentReplay).toBe(true);
    expect(second.transaction.id).toBe(first.transaction.id);
    expect(second.transaction.resultBalance?.amount).toBe("75.00");
    expect(mem.ledger).toHaveLength(1);
  });

  test("replay returns the balance observed at the time, not the current one", async () => {
    await useCase.execute(command(), http);
    await useCase.execute(command({ idempotencyKey: "prov:tx-2", externalTransactionId: "tx-2" }), http);
    const replay = await useCase.execute(command(), http);
    expect(replay.transaction.resultBalance?.amount).toBe("75.00");
    expect(mem.wallets.get(walletId)!.balance.amount).toBe("50.00");
  });

  test("same key with a different payload is a conflict, not a replay", async () => {
    await useCase.execute(command(), http);
    await expect(useCase.execute(command({ money: { amount: "26.00", currency: "BRL" } }), http)).rejects.toBeInstanceOf(IdempotencyConflictError);
    expect(mem.ledger).toHaveLength(1);
    expect(mem.wallets.get(walletId)!.balance.amount).toBe("75.00");
  });

  test("same external id under another key is a conflict", async () => {
    await useCase.execute(command(), http);
    await expect(useCase.execute(command({ idempotencyKey: "different" }), http)).rejects.toBeInstanceOf(IdempotencyConflictError);
  });

  test("inbox: same message id is a no-op; same id with another body is a conflict", async () => {
    const inbox = { consumerName: "c", messageId: "m1", payloadHash: "h1" };
    await useCase.execute(command(), { ...http, source: "sqs", inbox });
    const dup = await useCase.execute(command(), { ...http, source: "sqs", inbox });
    expect(dup.duplicateMessage).toBe(true);
    expect(mem.ledger).toHaveLength(1);
    await expect(useCase.execute(command(), { ...http, source: "sqs", inbox: { ...inbox, payloadHash: "h2" } })).rejects.toBeInstanceOf(InboxConflictError);
  });

  test("invalid input never reaches persistence", async () => {
    await expect(useCase.execute(command({ money: { amount: "1.005", currency: "BRL" } }), http)).rejects.toBeInstanceOf(ValidationError);
    await expect(useCase.execute(command({ kind: WagerTransactionKind.Opening }), http)).rejects.toBeInstanceOf(ValidationError);
    expect(mem.txs).toHaveLength(0);
  });
});
