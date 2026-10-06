import type { WagerResult } from "../../application/process-wager-transaction";
import type { ReconciliationReport } from "../../application/wallet-use-cases";
import type { WalletLedgerEntry } from "../../domain/ledger-entry";
import type { Wallet } from "../../domain/wallet";
import { type WagerTransaction, WagerTransactionStatus } from "../../domain/wager-transaction";

export const presentWallet = (w: Wallet) => ({
  id: w.id,
  playerId: w.playerId,
  balance: w.balance.toJSON(),
  version: w.version,
  createdAt: w.createdAt.toISOString(),
  updatedAt: w.updatedAt.toISOString(),
});

export const presentLedgerEntry = (e: WalletLedgerEntry) => ({
  id: e.id,
  transactionId: e.transactionId,
  walletVersion: e.walletVersion,
  direction: e.direction,
  money: e.money.toJSON(),
  balanceBefore: e.balanceBefore.toJSON(),
  balanceAfter: e.balanceAfter.toJSON(),
  createdAt: e.createdAt.toISOString(),
});

export const presentTransaction = (t: WagerTransaction) => ({
  id: t.id,
  providerId: t.providerId,
  externalTransactionId: t.externalTransactionId,
  idempotencyKey: t.idempotencyKey,
  walletId: t.walletId,
  playerId: t.playerId,
  roundId: t.roundId,
  gameId: t.gameId,
  kind: t.kind,
  money: t.money.toJSON(),
  status: t.status,
  ...(t.failureCode ? { failureCode: t.failureCode } : {}),
  ...(t.referenceExternalTransactionId ? { referenceExternalTransactionId: t.referenceExternalTransactionId } : {}),
  ...(t.referenceTransactionId ? { referenceTransactionId: t.referenceTransactionId } : {}),
  ...(t.resultBalance ? { balance: t.resultBalance.toJSON() } : {}),
  ...(t.status === WagerTransactionStatus.PendingReference
    ? { referenceAttempts: t.referenceAttempts, nextAttemptAt: t.nextAttemptAt?.toISOString() }
    : {}),
  createdAt: t.createdAt.toISOString(),
  ...(t.processedAt ? { processedAt: t.processedAt.toISOString() } : {}),
});

/**
 * Submission result. Status code by outcome (same for first answer and replay):
 *   PROCESSED → 200, PENDING_REFERENCE → 202, REJECTED → 422, FAILED → 500.
 */
export function presentSubmission(result: WagerResult): { status: number; body: Record<string, unknown> } {
  const t = result.transaction;
  const status = {
    [WagerTransactionStatus.Processed]: 200,
    [WagerTransactionStatus.PendingReference]: 202,
    [WagerTransactionStatus.Rejected]: 422,
    [WagerTransactionStatus.Failed]: 500,
    [WagerTransactionStatus.Pending]: 202,
  }[t.status];
  return {
    status,
    body: {
      transactionId: t.id,
      status: t.status,
      ...(t.failureCode ? { failureCode: t.failureCode } : {}),
      ...(t.resultBalance ? { balance: t.resultBalance.toJSON() } : {}),
      idempotentReplay: result.idempotentReplay,
    },
  };
}

export const presentReconciliation = (r: ReconciliationReport) => ({
  walletId: r.walletId,
  storedBalance: r.storedBalance.toJSON(),
  calculatedBalance: r.calculatedBalance.toJSON(),
  difference: r.difference.toJSON(),
  consistent: r.consistent,
  checkedEntries: r.checkedEntries,
});
