import { FailureCode } from "./failure-code";
import { LedgerDirection, type WalletLedgerEntry } from "./ledger-entry";
import type { Wallet } from "./wallet";
import {
  type ReferenceRetryPolicy,
  type WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from "./wager-transaction";

export interface SettlementContext {
  /** Resolved by (providerId, referenceExternalTransactionId); undefined if not (yet) known. */
  reference?: WagerTransaction;
  /** True when the reference already has a PROCESSED reversal (REFUND or ROLLBACK). */
  referenceAlreadyReversed: boolean;
  now: Date;
  newEntryId: () => string;
  retryPolicy: ReferenceRetryPolicy;
}

export type SettlementOutcome =
  | { type: "processed"; entry?: WalletLedgerEntry; reference?: WagerTransaction }
  | { type: "rejected"; code: FailureCode }
  | { type: "pending_reference" };

/**
 * Domain service applying the business rules of section 7 to a PENDING / PENDING_REFERENCE
 * transaction against its (already locked) wallet. Mutates both aggregates consistently:
 * either the wallet moved and a ledger entry is returned, or nothing moved.
 */
export function settleWagerTransaction(
  tx: WagerTransaction,
  wallet: Wallet,
  ctx: SettlementContext,
): SettlementOutcome {
  const reject = (code: FailureCode): SettlementOutcome => {
    tx.reject(code, ctx.now, wallet.balance);
    return { type: "rejected", code };
  };

  if (tx.walletId !== wallet.id) throw new Error("settlement invoked with the wrong wallet");
  if (wallet.playerId !== tx.playerId) return reject(FailureCode.WalletPlayerMismatch);
  if (wallet.currency !== tx.money.currency) return reject(FailureCode.CurrencyMismatch);

  const { reference } = ctx;
  if (tx.referenceExternalTransactionId) {
    // Missing (or still unresolved) reference: park it, or give up once the budget is spent.
    if (!reference || reference.status === WagerTransactionStatus.Pending ||
        reference.status === WagerTransactionStatus.PendingReference) {
      if (tx.markPendingReference(ctx.now, ctx.retryPolicy, wallet.balance)) return { type: "pending_reference" };
      return reject(FailureCode.ReferenceNotFound);
    }
    if (!tx.canReference(reference.kind)) return reject(FailureCode.ReferenceKindNotAllowed);
    if (!tx.sharesScopeWith(reference)) return reject(FailureCode.ReferenceScopeMismatch);
    if (reference.status !== WagerTransactionStatus.Processed) return reject(FailureCode.ReferenceNotProcessed);
    if (tx.isReversal()) {
      if (!tx.money.equals(reference.money)) return reject(FailureCode.ReversalAmountMismatch);
      if (ctx.referenceAlreadyReversed) return reject(FailureCode.ReferenceAlreadyReversed);
    }
  }

  if (!tx.affectsBalance()) {
    tx.markProcessed(reference?.id, ctx.now, wallet.balance);
    return { type: "processed", reference };
  }

  const direction = tx.ledgerDirectionFor(reference);
  if (direction === LedgerDirection.Debit && !wallet.canDebit(tx.money)) {
    return reject(
      tx.kind === WagerTransactionKind.Bet ? FailureCode.InsufficientFunds : FailureCode.ReversalInsufficientFunds,
    );
  }
  const entry = wallet.apply(direction, {
    entryId: ctx.newEntryId(),
    transactionId: tx.id,
    money: tx.money,
    at: ctx.now,
  });
  tx.markProcessed(reference?.id, ctx.now, wallet.balance);
  return { type: "processed", entry, reference };
}
