import { EntitySchema } from "@mikro-orm/core";

/*
 * Persistence records. Deliberately separate from the domain classes: the domain stays free of
 * ORM types/decorators and of the NUMERIC-as-string representation; mappers.ts converts both ways
 * through the domain `rehydrate` factories.
 */

export class WalletRecord {
  id!: string;
  playerId!: string;
  currency!: string;
  balance!: string;
  version!: number;
  createdAt!: Date;
  updatedAt!: Date;
}

export class WagerTransactionRecord {
  id!: string;
  providerId!: string;
  externalTransactionId!: string;
  idempotencyKey!: string;
  payloadHash!: string;
  walletId!: string;
  playerId!: string;
  roundId!: string;
  gameId!: string;
  kind!: string;
  amount!: string;
  currency!: string;
  referenceExternalTransactionId?: string | null;
  referenceTransactionId?: string | null;
  status!: string;
  failureCode?: string | null;
  resultBalance?: string | null;
  referenceAttempts!: number;
  nextAttemptAt?: Date | null;
  processedAt?: Date | null;
  createdAt!: Date;
  updatedAt!: Date;
}

export class LedgerEntryRecord {
  id!: string;
  walletId!: string;
  currency!: string;
  transactionId!: string;
  walletVersion!: number;
  direction!: string;
  amount!: string;
  balanceBefore!: string;
  balanceAfter!: string;
  createdAt!: Date;
}

export class InboxMessageRecord {
  consumerName!: string;
  messageId!: string;
  payloadHash!: string;
  receivedAt!: Date;
  processedAt?: Date | null;
}

export class OutboxMessageRecord {
  id!: string;
  aggregateId!: string;
  eventType!: string;
  payload!: Record<string, unknown>;
  occurredAt!: Date;
  attempts!: number;
  nextAttemptAt?: Date | null;
  publishedAt?: Date | null;
  lockedBy?: string | null;
  lockedUntil?: Date | null;
  lastError?: string | null;
}

const money = { type: "decimal", precision: 20, scale: 2 } as const;
const ts = { type: "datetime", columnType: "timestamptz" } as const;

export const WalletSchema = new EntitySchema<WalletRecord>({
  class: WalletRecord,
  tableName: "wallets",
  properties: {
    id: { type: "uuid", primary: true },
    playerId: { type: "string" },
    currency: { type: "string", columnType: "char(3)" },
    balance: { ...money },
    version: { type: "integer" },
    createdAt: { ...ts },
    updatedAt: { ...ts },
  },
});

export const WagerTransactionSchema = new EntitySchema<WagerTransactionRecord>({
  class: WagerTransactionRecord,
  tableName: "wager_transactions",
  properties: {
    id: { type: "uuid", primary: true },
    providerId: { type: "string" },
    externalTransactionId: { type: "string" },
    idempotencyKey: { type: "string" },
    payloadHash: { type: "string" },
    walletId: { type: "uuid" },
    playerId: { type: "string" },
    roundId: { type: "string" },
    gameId: { type: "string" },
    kind: { type: "string" },
    amount: { ...money },
    currency: { type: "string", columnType: "char(3)" },
    referenceExternalTransactionId: { type: "string", nullable: true },
    referenceTransactionId: { type: "uuid", nullable: true },
    status: { type: "string" },
    failureCode: { type: "string", nullable: true },
    resultBalance: { ...money, nullable: true },
    referenceAttempts: { type: "integer" },
    nextAttemptAt: { ...ts, nullable: true },
    processedAt: { ...ts, nullable: true },
    createdAt: { ...ts },
    updatedAt: { ...ts },
  },
});

export const LedgerEntrySchema = new EntitySchema<LedgerEntryRecord>({
  class: LedgerEntryRecord,
  tableName: "wallet_ledger_entries",
  properties: {
    id: { type: "uuid", primary: true },
    walletId: { type: "uuid" },
    currency: { type: "string", columnType: "char(3)" },
    transactionId: { type: "uuid" },
    walletVersion: { type: "integer" },
    direction: { type: "string" },
    amount: { ...money },
    balanceBefore: { ...money },
    balanceAfter: { ...money },
    createdAt: { ...ts },
  },
});

export const InboxMessageSchema = new EntitySchema<InboxMessageRecord>({
  class: InboxMessageRecord,
  tableName: "inbox_messages",
  properties: {
    consumerName: { type: "string", primary: true },
    messageId: { type: "string", primary: true },
    payloadHash: { type: "string" },
    receivedAt: { ...ts },
    processedAt: { ...ts, nullable: true },
  },
});

export const OutboxMessageSchema = new EntitySchema<OutboxMessageRecord>({
  class: OutboxMessageRecord,
  tableName: "outbox_messages",
  properties: {
    id: { type: "uuid", primary: true },
    aggregateId: { type: "string" },
    eventType: { type: "string" },
    payload: { type: "json" },
    occurredAt: { ...ts },
    attempts: { type: "integer" },
    nextAttemptAt: { ...ts, nullable: true },
    publishedAt: { ...ts, nullable: true },
    lockedBy: { type: "string", nullable: true },
    lockedUntil: { ...ts, nullable: true },
    lastError: { type: "text", nullable: true },
  },
});

export const ENTITY_SCHEMAS = [
  WalletSchema,
  WagerTransactionSchema,
  LedgerEntrySchema,
  InboxMessageSchema,
  OutboxMessageSchema,
];
