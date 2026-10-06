import type { InboxMessage } from "../domain/inbox-message";
import type { WalletLedgerEntry } from "../domain/ledger-entry";
import type { Money } from "../domain/money";
import type { OutboxMessage } from "../domain/outbox-message";
import type { Wallet } from "../domain/wallet";
import type { WagerTransaction } from "../domain/wager-transaction";

export interface Clock {
  now(): Date;
}

export interface IdGenerator {
  /** Time-ordered UUID (v7). */
  next(): string;
}

export interface WalletLock {
  wallet?: Wallet;
  /** True when another transaction held the row lock and we had to wait for it. */
  contended: boolean;
}

export interface WalletRepository {
  findById(id: string): Promise<Wallet | undefined>;
  /** SELECT … FOR UPDATE: serialises every balance change of this wallet (and only this wallet). */
  lockForUpdate(id: string): Promise<WalletLock>;
  /** SELECT … FOR SHARE: blocks writers while a consistent read (reconciliation) runs. */
  lockForShare(id: string): Promise<Wallet | undefined>;
  insert(wallet: Wallet): Promise<void>;
  /** Conditional update `WHERE version = expectedVersion`; throws ConcurrencyConflictError otherwise. */
  saveBalance(wallet: Wallet, expectedVersion: number): Promise<void>;
}

export interface WagerTransactionRepository {
  findById(id: string): Promise<WagerTransaction | undefined>;
  findByIdempotencyKey(key: string): Promise<WagerTransaction | undefined>;
  findByExternalId(providerId: string, externalTransactionId: string): Promise<WagerTransaction | undefined>;
  hasProcessedReversal(referenceTransactionId: string): Promise<boolean>;
  insert(tx: WagerTransaction): Promise<void>;
  /** Persists status-related fields (status, failure, reference, attempts, result balance). */
  updateState(tx: WagerTransaction): Promise<void>;
  /** Makes PENDING_REFERENCE transactions waiting for (providerId, externalId) due immediately. */
  wakeDependents(providerId: string, externalTransactionId: string, now: Date): Promise<number>;
  findDuePendingReferenceIds(now: Date, limit: number): Promise<string[]>;
}

export interface LedgerSummary {
  credits: Money;
  debits: Money;
  entries: number;
}

export interface LedgerRepository {
  insert(entry: WalletLedgerEntry): Promise<void>;
  /** Entries ordered by wallet version, strictly after `afterVersion`. */
  page(walletId: string, afterVersion: number, limit: number): Promise<WalletLedgerEntry[]>;
  summarize(walletId: string, currency: string): Promise<LedgerSummary>;
}

export type InboxReceipt = { duplicate: false } | { duplicate: true; existing: InboxMessage };

export interface InboxRepository {
  /** INSERT … ON CONFLICT DO NOTHING; returns the stored row when the message was already consumed. */
  receive(message: InboxMessage): Promise<InboxReceipt>;
}

export interface OutboxRepository {
  add(messages: OutboxMessage[]): Promise<void>;
}

/** Repositories bound to one open SQL transaction. */
export interface TransactionalRepositories {
  wallets: WalletRepository;
  transactions: WagerTransactionRepository;
  ledger: LedgerRepository;
  inbox: InboxRepository;
  outbox: OutboxRepository;
}

export interface UnitOfWork {
  /** Runs `work` inside one SQL transaction; commits when it resolves, rolls back when it throws. */
  run<T>(work: (repos: TransactionalRepositories) => Promise<T>): Promise<T>;
}

/** Repositories outside any explicit transaction (single-statement reads). */
export interface ReadRepositories {
  wallets: Pick<WalletRepository, "findById">;
  transactions: Pick<WagerTransactionRepository, "findById" | "findByExternalId" | "findDuePendingReferenceIds">;
  ledger: Pick<LedgerRepository, "page">;
}

/** Business metrics the application layer reports. Implemented with prom-client in infrastructure. */
export interface WageringMetrics {
  transactionFinished(kind: string, status: string, source: string): void;
  duplicateDetected(kind: "idempotent_replay" | "inbox_redelivery" | "idempotency_conflict"): void;
  retry(operation: string, reason: string): void;
  lockConflict(type: "contended" | "timeout" | "version" | "deadlock" | "unique_race"): void;
  processingLatency(source: string, seconds: number): void;
  reconciliation(consistent: boolean): void;
}

export interface AppLogger {
  info(msg: string, fields?: Record<string, unknown>): void;
  warn(msg: string, fields?: Record<string, unknown>): void;
  error(msg: string, fields?: Record<string, unknown>): void;
  debug(msg: string, fields?: Record<string, unknown>): void;
}

export const CLOCK = Symbol("Clock");
export const ID_GENERATOR = Symbol("IdGenerator");
export const UNIT_OF_WORK = Symbol("UnitOfWork");
export const READ_REPOSITORIES = Symbol("ReadRepositories");
export const METRICS = Symbol("WageringMetrics");
export const LOGGER = Symbol("AppLogger");
export const REFERENCE_RETRY_POLICY = Symbol("ReferenceRetryPolicy");
