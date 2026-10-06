import type { FailureCode } from "../failure-code";
import type { LedgerDirection, WalletLedgerEntry } from "../ledger-entry";
import type { MoneyProps } from "../money";
import type { Wallet } from "../wallet";
import type { WagerTransaction, WagerTransactionKind } from "../wager-transaction";
import { type EventContext, IntegrationEvent } from "./integration-event";

interface TransactionData {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
  referenceExternalTransactionId?: string;
}

function transactionData(tx: WagerTransaction): TransactionData {
  return {
    transactionId: tx.id,
    providerId: tx.providerId,
    externalTransactionId: tx.externalTransactionId,
    walletId: tx.walletId,
    playerId: tx.playerId,
    roundId: tx.roundId,
    gameId: tx.gameId,
    kind: tx.kind,
    money: tx.money.toJSON(),
    ...(tx.referenceExternalTransactionId ? { referenceExternalTransactionId: tx.referenceExternalTransactionId } : {}),
  };
}

function props<T>(aggregateId: string, ctx: EventContext, data: T) {
  return { ...ctx, aggregateId, data };
}

export interface WagerTransactionProcessedData extends TransactionData {
  referenceTransactionId?: string;
  balance: MoneyProps;
  processedAt: string;
}

/** Any applied transaction, including LOSS (no balance change). */
export class WagerTransactionProcessed extends IntegrationEvent<WagerTransactionProcessedData> {
  readonly eventType = "WagerTransactionProcessed";
  readonly version = 1;

  static from(tx: WagerTransaction, ctx: EventContext): WagerTransactionProcessed {
    return new WagerTransactionProcessed(
      props(tx.walletId, ctx, {
        ...transactionData(tx),
        ...(tx.referenceTransactionId ? { referenceTransactionId: tx.referenceTransactionId } : {}),
        balance: tx.resultBalance!.toJSON(),
        processedAt: tx.processedAt!.toISOString(),
      }),
    );
  }
}

export interface WagerTransactionRejectedData extends TransactionData {
  failureCode: FailureCode;
  balance: MoneyProps;
}

export class WagerTransactionRejected extends IntegrationEvent<WagerTransactionRejectedData> {
  readonly eventType = "WagerTransactionRejected";
  readonly version = 1;

  static from(tx: WagerTransaction, ctx: EventContext): WagerTransactionRejected {
    return new WagerTransactionRejected(
      props(tx.walletId, ctx, {
        ...transactionData(tx),
        failureCode: tx.failureCode!,
        balance: tx.resultBalance!.toJSON(),
      }),
    );
  }
}

export interface WagerTransactionPendingReferenceData extends TransactionData {
  referenceExternalTransactionId: string;
  nextAttemptAt: string;
}

export class WagerTransactionPendingReference extends IntegrationEvent<WagerTransactionPendingReferenceData> {
  readonly eventType = "WagerTransactionPendingReference";
  readonly version = 1;

  static from(tx: WagerTransaction, ctx: EventContext): WagerTransactionPendingReference {
    return new WagerTransactionPendingReference(
      props(tx.walletId, ctx, {
        ...transactionData(tx),
        referenceExternalTransactionId: tx.referenceExternalTransactionId!,
        nextAttemptAt: tx.nextAttemptAt!.toISOString(),
      }),
    );
  }
}

export interface WalletBalanceChangedData {
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  walletVersion: number;
}

/** Emitted only when the balance actually changes (exactly one per ledger entry). */
export class WalletBalanceChanged extends IntegrationEvent<WalletBalanceChangedData> {
  readonly eventType = "WalletBalanceChanged";
  readonly version = 1;

  static from(wallet: Wallet, entry: WalletLedgerEntry, ctx: EventContext): WalletBalanceChanged {
    return new WalletBalanceChanged(
      props(wallet.id, ctx, {
        walletId: wallet.id,
        transactionId: entry.transactionId,
        direction: entry.direction,
        money: entry.money.toJSON(),
        balanceBefore: entry.balanceBefore.toJSON(),
        balanceAfter: entry.balanceAfter.toJSON(),
        walletVersion: entry.walletVersion,
      }),
    );
  }
}
