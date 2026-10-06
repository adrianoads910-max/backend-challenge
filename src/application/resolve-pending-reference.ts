import { Inject, Injectable } from "@nestjs/common";
import { FailureCode } from "../domain/failure-code";
import { settleWagerTransaction } from "../domain/wager-settlement";
import { type ReferenceRetryPolicy, WagerTransactionStatus } from "../domain/wager-transaction";
import { ProcessWagerTransaction } from "./process-wager-transaction";
import {
  type AppLogger,
  CLOCK,
  type Clock,
  ID_GENERATOR,
  type IdGenerator,
  LOGGER,
  METRICS,
  READ_REPOSITORIES,
  type ReadRepositories,
  REFERENCE_RETRY_POLICY,
  UNIT_OF_WORK,
  type UnitOfWork,
  type WageringMetrics,
} from "./ports";

export type ResolutionResult = "processed" | "rejected" | "still_pending" | "skipped";

/**
 * Re-attempts a PENDING_REFERENCE transaction. Lock order is the same as the main path
 * (wallet row first, then the transaction row), so it can never deadlock with it; and the state
 * is re-read under the lock, so concurrent resolvers on several instances are harmless.
 */
@Injectable()
export class ResolvePendingReference {
  constructor(
    @Inject(UNIT_OF_WORK) private readonly uow: UnitOfWork,
    @Inject(READ_REPOSITORIES) private readonly read: ReadRepositories,
    @Inject(CLOCK) private readonly clock: Clock,
    @Inject(ID_GENERATOR) private readonly ids: IdGenerator,
    @Inject(METRICS) private readonly metrics: WageringMetrics,
    @Inject(LOGGER) private readonly logger: AppLogger,
    @Inject(REFERENCE_RETRY_POLICY) private readonly retryPolicy: ReferenceRetryPolicy,
    private readonly processor: ProcessWagerTransaction,
  ) {}

  dueIds(limit: number): Promise<string[]> {
    return this.read.transactions.findDuePendingReferenceIds(this.clock.now(), limit);
  }

  async execute(transactionId: string, correlationId: string): Promise<ResolutionResult> {
    const result = await this.uow.run(async (repos) => {
      const snapshot = await repos.transactions.findById(transactionId);
      if (!snapshot || snapshot.status !== WagerTransactionStatus.PendingReference) return { type: "skipped" as const };

      const { wallet, contended } = await repos.wallets.lockForUpdate(snapshot.walletId);
      if (contended) this.metrics.lockConflict("contended");
      if (!wallet) throw new Error(`wallet ${snapshot.walletId} vanished`);

      const now = this.clock.now();
      const tx = await repos.transactions.findById(transactionId);
      if (!tx || tx.status !== WagerTransactionStatus.PendingReference) return { type: "skipped" as const };
      if (tx.nextAttemptAt && tx.nextAttemptAt > now) return { type: "skipped" as const };

      const expectedVersion = wallet.version;
      const reference = await repos.transactions.findByExternalId(tx.providerId, tx.referenceExternalTransactionId!);
      const referenceAlreadyReversed =
        reference && tx.isReversal() ? await repos.transactions.hasProcessedReversal(reference.id) : false;

      const outcome = settleWagerTransaction(tx, wallet, {
        reference,
        referenceAlreadyReversed,
        now,
        newEntryId: () => this.ids.next(),
        retryPolicy: this.retryPolicy,
      });
      await repos.transactions.updateState(tx);
      await this.processor.persistOutcome(
        repos, tx, wallet, expectedVersion, outcome, { correlationId, causationId: tx.id }, now, false,
      );
      return { type: outcome.type, tx };
    });

    if (result.type === "skipped") return "skipped";
    const tx = result.tx;
    const fields = { transactionId: tx.id, walletId: tx.walletId, providerId: tx.providerId, status: tx.status };
    if (result.type === "pending_reference") {
      this.metrics.retry("resolve_pending_reference", "reference_missing");
      this.logger.info("reference still missing", { ...fields, attempts: tx.referenceAttempts });
      return "still_pending";
    }
    this.metrics.transactionFinished(tx.kind, tx.status, "pending_reference_worker");
    this.logger.info("pending reference resolved", { ...fields, failureCode: tx.failureCode });
    return result.type;
  }

  /**
   * Terminal FAILED for a transaction whose resolution keeps throwing a non-transient error
   * (data corruption, bug). It stays auditable and stops consuming worker capacity.
   */
  async markFailed(transactionId: string): Promise<void> {
    await this.uow.run(async (repos) => {
      const snapshot = await repos.transactions.findById(transactionId);
      if (!snapshot || snapshot.status !== WagerTransactionStatus.PendingReference) return;
      await repos.wallets.lockForUpdate(snapshot.walletId);
      const tx = await repos.transactions.findById(transactionId);
      if (!tx || tx.status !== WagerTransactionStatus.PendingReference) return;
      tx.fail(FailureCode.ProcessingFailed, this.clock.now());
      await repos.transactions.updateState(tx);
    });
    this.metrics.transactionFinished("unknown", WagerTransactionStatus.Failed, "pending_reference_worker");
    this.logger.error("pending transaction marked FAILED", { transactionId });
  }
}
