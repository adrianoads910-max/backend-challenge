import type { FailureCode } from "../../domain/failure-code";
import { InboxMessage } from "../../domain/inbox-message";
import { type LedgerDirection, WalletLedgerEntry } from "../../domain/ledger-entry";
import { Money } from "../../domain/money";
import type { OutboxMessage } from "../../domain/outbox-message";
import { Wallet } from "../../domain/wallet";
import { WagerTransaction, type WagerTransactionKind, type WagerTransactionStatus } from "../../domain/wager-transaction";
import type {
  InboxMessageRecord,
  LedgerEntryRecord,
  OutboxMessageRecord,
  WagerTransactionRecord,
  WalletRecord,
} from "./records";

/** NUMERIC comes back from pg as an exact decimal string; never goes through `number`. */
const money = (amount: string, currency: string) => Money.from({ amount: normalize(amount), currency });
// numeric(20,2) is always rendered with 2 decimals by pg, but be defensive about scale.
const normalize = (amount: string) => (amount.includes(".") ? amount : `${amount}.00`);
const opt = <T>(v: T | null | undefined): T | undefined => (v === null ? undefined : v);

export const walletToDomain = (r: WalletRecord): Wallet =>
  Wallet.rehydrate({
    id: r.id,
    playerId: r.playerId,
    currency: r.currency,
    balance: money(r.balance, r.currency),
    version: r.version,
    createdAt: r.createdAt,
    updatedAt: r.updatedAt,
  });

export const walletToRecord = (w: Wallet): WalletRecord => ({
  id: w.id,
  playerId: w.playerId,
  currency: w.currency,
  balance: w.balance.amount,
  version: w.version,
  createdAt: w.createdAt,
  updatedAt: w.updatedAt,
});

export const transactionToDomain = (r: WagerTransactionRecord): WagerTransaction =>
  WagerTransaction.rehydrate({
    id: r.id,
    providerId: r.providerId,
    externalTransactionId: r.externalTransactionId,
    idempotencyKey: r.idempotencyKey,
    payloadHash: r.payloadHash,
    walletId: r.walletId,
    playerId: r.playerId,
    roundId: r.roundId,
    gameId: r.gameId,
    kind: r.kind as WagerTransactionKind,
    money: money(r.amount, r.currency),
    referenceExternalTransactionId: opt(r.referenceExternalTransactionId),
    createdAt: r.createdAt,
    status: r.status as WagerTransactionStatus,
    referenceTransactionId: opt(r.referenceTransactionId),
    failureCode: opt(r.failureCode) as FailureCode | undefined,
    processedAt: opt(r.processedAt),
    resultBalance: r.resultBalance == null ? undefined : money(r.resultBalance, r.currency),
    referenceAttempts: r.referenceAttempts,
    nextAttemptAt: opt(r.nextAttemptAt),
  });

/** Mutable (state) columns of a transaction — the only ones `updateState` may write. */
export const transactionStateToRecord = (t: WagerTransaction, now: Date) => ({
  status: t.status,
  referenceTransactionId: t.referenceTransactionId ?? null,
  failureCode: t.failureCode ?? null,
  resultBalance: t.resultBalance?.amount ?? null,
  referenceAttempts: t.referenceAttempts,
  nextAttemptAt: t.nextAttemptAt ?? null,
  processedAt: t.processedAt ?? null,
  updatedAt: now,
});

export const transactionToRecord = (t: WagerTransaction): WagerTransactionRecord => ({
  id: t.id,
  providerId: t.providerId,
  externalTransactionId: t.externalTransactionId,
  idempotencyKey: t.idempotencyKey,
  payloadHash: t.payloadHash,
  walletId: t.walletId,
  playerId: t.playerId,
  roundId: t.roundId,
  gameId: t.gameId,
  kind: t.kind,
  amount: t.money.amount,
  currency: t.money.currency,
  referenceExternalTransactionId: t.referenceExternalTransactionId ?? null,
  createdAt: t.createdAt,
  ...transactionStateToRecord(t, t.createdAt),
});

export const ledgerToDomain = (r: LedgerEntryRecord): WalletLedgerEntry =>
  WalletLedgerEntry.rehydrate({
    id: r.id,
    walletId: r.walletId,
    transactionId: r.transactionId,
    walletVersion: r.walletVersion,
    direction: r.direction as LedgerDirection,
    money: money(r.amount, r.currency),
    balanceBefore: money(r.balanceBefore, r.currency),
    balanceAfter: money(r.balanceAfter, r.currency),
    createdAt: r.createdAt,
  });

export const ledgerToRecord = (e: WalletLedgerEntry): LedgerEntryRecord => ({
  id: e.id,
  walletId: e.walletId,
  currency: e.money.currency,
  transactionId: e.transactionId,
  walletVersion: e.walletVersion,
  direction: e.direction,
  amount: e.money.amount,
  balanceBefore: e.balanceBefore.amount,
  balanceAfter: e.balanceAfter.amount,
  createdAt: e.createdAt,
});

export const inboxToDomain = (r: InboxMessageRecord): InboxMessage =>
  InboxMessage.rehydrate({
    consumerName: r.consumerName,
    messageId: r.messageId,
    payloadHash: r.payloadHash,
    receivedAt: r.receivedAt,
    processedAt: opt(r.processedAt),
  });

export const outboxToRecord = (m: OutboxMessage): OutboxMessageRecord => ({
  id: m.id,
  aggregateId: m.aggregateId,
  eventType: m.eventType,
  // The domain payload is frozen; MikroORM normalises params in place, so hand it a copy.
  payload: structuredClone(m.payload) as Record<string, unknown>,
  occurredAt: m.occurredAt,
  attempts: m.attempts,
  nextAttemptAt: m.nextAttemptAt ?? null,
  publishedAt: m.publishedAt ?? null,
});
