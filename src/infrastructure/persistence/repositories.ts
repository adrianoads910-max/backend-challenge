import { LockMode } from "@mikro-orm/core";
import type { EntityManager } from "@mikro-orm/postgresql";
import {
  ConcurrencyConflictError,
  DuplicateRaceError,
  WalletAlreadyExistsError,
} from "../../application/errors";
import type {
  InboxReceipt,
  InboxRepository,
  LedgerRepository,
  LedgerSummary,
  OutboxRepository,
  TransactionalRepositories,
  WagerTransactionRepository,
  WalletLock,
  WalletRepository,
} from "../../application/ports";
import type { InboxMessage } from "../../domain/inbox-message";
import type { WalletLedgerEntry } from "../../domain/ledger-entry";
import { Money } from "../../domain/money";
import type { OutboxMessage } from "../../domain/outbox-message";
import type { Wallet } from "../../domain/wallet";
import type { WagerTransaction } from "../../domain/wager-transaction";
import { WagerTransactionStatus } from "../../domain/wager-transaction";
import {
  inboxToDomain,
  ledgerToDomain,
  ledgerToRecord,
  outboxToRecord,
  transactionStateToRecord,
  transactionToDomain,
  transactionToRecord,
  walletToDomain,
  walletToRecord,
} from "./mappers";
import {
  InboxMessageRecord,
  LedgerEntryRecord,
  OutboxMessageRecord,
  WagerTransactionRecord,
  WalletRecord,
} from "./records";
import { pgErrorInfo } from "./pg-errors";

/*
 * Repositories go through MikroORM's QueryBuilder / native operations rather than the Unit of
 * Work's dirty checking: every write here is a deliberate, single SQL statement whose locking and
 * conflict semantics must be obvious in review (FOR UPDATE, ON CONFLICT, WHERE version = ?).
 * Results are read with `execute()` (no identity map), so a re-read under a lock always hits the DB.
 */

const RACE_CONSTRAINTS = new Set([
  "wager_tx_idempotency_key_uq",
  "wager_tx_provider_external_id_uq",
  "wager_tx_single_reversal_uq",
  "inbox_messages_pkey",
]);

function rethrowRace(err: unknown): never {
  const info = pgErrorInfo(err);
  if (info?.code === "23505" && info.constraint && RACE_CONSTRAINTS.has(info.constraint)) {
    throw new DuplicateRaceError(info.constraint);
  }
  if (info?.code === "23505" && info.constraint === "ledger_wallet_version_uq") {
    throw new ConcurrencyConflictError("ledger version already taken");
  }
  throw err;
}

export class PgWalletRepository implements WalletRepository {
  constructor(private readonly em: EntityManager) {}

  async findById(id: string): Promise<Wallet | undefined> {
    const row = await this.em.createQueryBuilder(WalletRecord).select("*").where({ id }).execute("get");
    return row ? walletToDomain(row) : undefined;
  }

  async lockForUpdate(id: string): Promise<WalletLock> {
    // Probe with SKIP LOCKED first: an empty result on an existing row means someone else holds
    // the lock (counted as a lock conflict), then block on a plain FOR UPDATE (bounded by lock_timeout).
    const probe = await this.em
      .createQueryBuilder(WalletRecord)
      .select("*")
      .where({ id })
      .setLockMode(LockMode.PESSIMISTIC_PARTIAL_WRITE)
      .execute("get");
    if (probe) return { wallet: walletToDomain(probe), contended: false };
    const row = await this.em
      .createQueryBuilder(WalletRecord)
      .select("*")
      .where({ id })
      .setLockMode(LockMode.PESSIMISTIC_WRITE)
      .execute("get");
    return { wallet: row ? walletToDomain(row) : undefined, contended: row !== undefined && row !== null };
  }

  async lockForShare(id: string): Promise<Wallet | undefined> {
    const row = await this.em
      .createQueryBuilder(WalletRecord)
      .select("*")
      .where({ id })
      .setLockMode(LockMode.PESSIMISTIC_READ)
      .execute("get");
    return row ? walletToDomain(row) : undefined;
  }

  async insert(wallet: Wallet): Promise<void> {
    try {
      await this.em.insert(WalletRecord, walletToRecord(wallet));
    } catch (err) {
      if (pgErrorInfo(err)?.constraint === "wallets_player_currency_uq") {
        throw new WalletAlreadyExistsError(
          `player ${wallet.playerId} already has a ${wallet.currency} wallet`,
        );
      }
      throw err;
    }
  }

  async saveBalance(wallet: Wallet, expectedVersion: number): Promise<void> {
    const affected = await this.em.nativeUpdate(
      WalletRecord,
      { id: wallet.id, version: expectedVersion },
      { balance: wallet.balance.amount, version: wallet.version, updatedAt: wallet.updatedAt },
    );
    if (affected !== 1) {
      throw new ConcurrencyConflictError(`wallet ${wallet.id} is no longer at version ${expectedVersion}`);
    }
  }
}

export class PgWagerTransactionRepository implements WagerTransactionRepository {
  constructor(private readonly em: EntityManager) {}

  private async one(where: Partial<WagerTransactionRecord>): Promise<WagerTransaction | undefined> {
    const row = await this.em.createQueryBuilder(WagerTransactionRecord).select("*").where(where).execute("get");
    return row ? transactionToDomain(row) : undefined;
  }

  findById(id: string) {
    return this.one({ id });
  }

  findByIdempotencyKey(idempotencyKey: string) {
    return this.one({ idempotencyKey });
  }

  findByExternalId(providerId: string, externalTransactionId: string) {
    return this.one({ providerId, externalTransactionId });
  }

  async hasProcessedReversal(referenceTransactionId: string): Promise<boolean> {
    const row = await this.em
      .createQueryBuilder(WagerTransactionRecord)
      .select("id")
      .where({ referenceTransactionId, status: WagerTransactionStatus.Processed, kind: { $in: ["REFUND", "ROLLBACK"] } })
      .limit(1)
      .execute("get");
    return Boolean(row);
  }

  async insert(tx: WagerTransaction): Promise<void> {
    try {
      await this.em.insert(WagerTransactionRecord, transactionToRecord(tx));
    } catch (err) {
      rethrowRace(err);
    }
  }

  async updateState(tx: WagerTransaction): Promise<void> {
    try {
      await this.em.nativeUpdate(WagerTransactionRecord, { id: tx.id }, transactionStateToRecord(tx, new Date()));
    } catch (err) {
      rethrowRace(err);
    }
  }

  async wakeDependents(providerId: string, externalTransactionId: string, now: Date): Promise<number> {
    return this.em.nativeUpdate(
      WagerTransactionRecord,
      {
        providerId,
        referenceExternalTransactionId: externalTransactionId,
        status: WagerTransactionStatus.PendingReference,
        nextAttemptAt: { $gt: now },
      },
      { nextAttemptAt: now, updatedAt: now },
    );
  }

  async findDuePendingReferenceIds(now: Date, limit: number): Promise<string[]> {
    const rows = await this.em
      .createQueryBuilder(WagerTransactionRecord)
      .select("id")
      .where({ status: WagerTransactionStatus.PendingReference, nextAttemptAt: { $lte: now } })
      .orderBy({ nextAttemptAt: "asc" })
      .limit(limit)
      .execute("all");
    return rows.map((r) => r.id);
  }
}

export class PgLedgerRepository implements LedgerRepository {
  constructor(private readonly em: EntityManager) {}

  async insert(entry: WalletLedgerEntry): Promise<void> {
    try {
      await this.em.insert(LedgerEntryRecord, ledgerToRecord(entry));
    } catch (err) {
      rethrowRace(err);
    }
  }

  async page(walletId: string, afterVersion: number, limit: number): Promise<WalletLedgerEntry[]> {
    const rows = await this.em
      .createQueryBuilder(LedgerEntryRecord)
      .select("*")
      .where({ walletId, walletVersion: { $gt: afterVersion } })
      .orderBy({ walletVersion: "asc" })
      .limit(limit)
      .execute("all");
    return rows.map(ledgerToDomain);
  }

  async summarize(walletId: string, currency: string): Promise<LedgerSummary> {
    // Sums are computed by PostgreSQL in NUMERIC and returned as text: exact, no float on the way.
    const [row] = await this.em.execute<{ credits: string; debits: string; entries: string }[]>(
      `select coalesce(sum(amount) filter (where direction = 'CREDIT'), 0)::numeric(20,2)::text as credits,
              coalesce(sum(amount) filter (where direction = 'DEBIT'), 0)::numeric(20,2)::text  as debits,
              count(*)::text as entries
         from wallet_ledger_entries where wallet_id = ?`,
      [walletId],
    );
    return {
      credits: Money.from({ amount: row!.credits, currency }),
      debits: Money.from({ amount: row!.debits, currency }),
      entries: Number.parseInt(row!.entries, 10),
    };
  }
}

export class PgInboxRepository implements InboxRepository {
  constructor(private readonly em: EntityManager) {}

  async receive(message: InboxMessage): Promise<InboxReceipt> {
    // The row is written already "processed": it only becomes visible if the whole business
    // transaction commits, so "present" == "consumed".
    message.markProcessed(message.receivedAt);
    const inserted = await this.em
      .createQueryBuilder(InboxMessageRecord)
      .insert({
        consumerName: message.consumerName,
        messageId: message.messageId,
        payloadHash: message.payloadHash,
        receivedAt: message.receivedAt,
        processedAt: message.processedAt,
      })
      .onConflict(["consumerName", "messageId"])
      .ignore()
      .execute("run");
    if (inserted.affectedRows === 1) return { duplicate: false };
    const existing = await this.em
      .createQueryBuilder(InboxMessageRecord)
      .select("*")
      .where({ consumerName: message.consumerName, messageId: message.messageId })
      .execute("get");
    return { duplicate: true, existing: inboxToDomain(existing!) };
  }
}

export class PgOutboxRepository implements OutboxRepository {
  constructor(private readonly em: EntityManager) {}

  async add(messages: OutboxMessage[]): Promise<void> {
    if (messages.length === 0) return;
    await this.em.insertMany(OutboxMessageRecord, messages.map(outboxToRecord));
  }
}

export function transactionalRepositories(em: EntityManager): TransactionalRepositories {
  return {
    wallets: new PgWalletRepository(em),
    transactions: new PgWagerTransactionRepository(em),
    ledger: new PgLedgerRepository(em),
    inbox: new PgInboxRepository(em),
    outbox: new PgOutboxRepository(em),
  };
}
