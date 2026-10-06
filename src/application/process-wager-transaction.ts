import { Inject, Injectable } from "@nestjs/common";
import { DomainError } from "../domain/errors";
import type { EventContext, IntegrationEvent } from "../domain/events/integration-event";
import {
  WagerTransactionPendingReference,
  WagerTransactionProcessed,
  WagerTransactionRejected,
  WalletBalanceChanged,
} from "../domain/events/wagering-events";
import { InboxMessage } from "../domain/inbox-message";
import { Money, type MoneyProps } from "../domain/money";
import { OutboxMessage } from "../domain/outbox-message";
import type { Wallet } from "../domain/wallet";
import { type SettlementOutcome, settleWagerTransaction } from "../domain/wager-settlement";
import {
  type ReferenceRetryPolicy,
  WagerTransaction,
  type WagerTransactionKind,
  WagerTransactionStatus,
} from "../domain/wager-transaction";
import {
  ConcurrencyConflictError,
  DuplicateRaceError,
  IdempotencyConflictError,
  InboxConflictError,
  NotFoundError,
  TransientError,
  ValidationError,
} from "./errors";
import { sha256Canonical } from "./payload-hash";
import {
  type AppLogger,
  CLOCK,
  type Clock,
  ID_GENERATOR,
  type IdGenerator,
  LOGGER,
  METRICS,
  REFERENCE_RETRY_POLICY,
  type TransactionalRepositories,
  UNIT_OF_WORK,
  type UnitOfWork,
  type WageringMetrics,
} from "./ports";

export interface SubmitWagerTransactionCommand {
  idempotencyKey: string;
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
  referenceExternalTransactionId?: string;
}

export interface ProcessOptions {
  source: "http" | "sqs";
  correlationId: string;
  causationId?: string;
  /** Present when the command came from the queue: dedup record written in the same SQL transaction. */
  inbox?: { consumerName: string; messageId: string; payloadHash: string };
}

export interface WagerResult {
  transaction: WagerTransaction;
  idempotentReplay: boolean;
  /** The queue message itself was already consumed (inbox hit). */
  duplicateMessage: boolean;
}

/** Business fields that identify the operation. Header/transport metadata are deliberately excluded. */
export function wagerPayloadHash(cmd: SubmitWagerTransactionCommand, money: Money): string {
  return sha256Canonical({
    providerId: cmd.providerId,
    externalTransactionId: cmd.externalTransactionId,
    playerId: cmd.playerId,
    walletId: cmd.walletId,
    roundId: cmd.roundId,
    gameId: cmd.gameId,
    kind: cmd.kind,
    money: money.toJSON(),
    referenceExternalTransactionId: cmd.referenceExternalTransactionId,
  });
}

const MAX_ATTEMPTS = 3;

/**
 * The single entry point for provider operations — used by both the HTTP controller and the SQS
 * consumer. One call = one SQL transaction containing: inbox record (queue only), transaction row,
 * wallet update, ledger entry and outbox events. Concurrency unit: the wallet row lock.
 */
@Injectable()
export class ProcessWagerTransaction {
  constructor(
    @Inject(UNIT_OF_WORK) private readonly uow: UnitOfWork,
    @Inject(CLOCK) private readonly clock: Clock,
    @Inject(ID_GENERATOR) private readonly ids: IdGenerator,
    @Inject(METRICS) private readonly metrics: WageringMetrics,
    @Inject(LOGGER) private readonly logger: AppLogger,
    @Inject(REFERENCE_RETRY_POLICY) private readonly retryPolicy: ReferenceRetryPolicy,
  ) {}

  async execute(cmd: SubmitWagerTransactionCommand, opts: ProcessOptions): Promise<WagerResult> {
    const started = performance.now();
    this.buildCandidate(cmd); // fail fast on invalid input, before opening a transaction

    for (let attempt = 1; ; attempt++) {
      try {
        // Settlement mutates the candidate, so every attempt starts from a fresh one.
        const candidate = this.buildCandidate(cmd);
        const result = await this.uow.run((repos) => this.process(repos, candidate, opts));
        this.report(result, opts, started);
        return result;
      } catch (err) {
        const retryable = err instanceof DuplicateRaceError || err instanceof ConcurrencyConflictError ||
          (err instanceof TransientError && err.reason === "deadlock");
        if (!retryable || attempt >= MAX_ATTEMPTS) {
          if (err instanceof DuplicateRaceError || err instanceof ConcurrencyConflictError) {
            throw new TransientError("could not settle a concurrent race", "race_exhausted", err);
          }
          throw err;
        }
        const reason = err instanceof DuplicateRaceError ? "unique_race" : err instanceof ConcurrencyConflictError ? "version" : "deadlock";
        this.metrics.lockConflict(reason);
        this.metrics.retry("process_wager_transaction", reason);
        this.logger.warn("retrying wager transaction after race", { reason, attempt });
      }
    }
  }

  /** Pure validation + domain construction, before touching the database. */
  private buildCandidate(cmd: SubmitWagerTransactionCommand): WagerTransaction {
    try {
      const money = Money.from(cmd.money);
      return WagerTransaction.create({
        id: this.ids.next(),
        providerId: cmd.providerId,
        externalTransactionId: cmd.externalTransactionId,
        idempotencyKey: cmd.idempotencyKey,
        payloadHash: wagerPayloadHash(cmd, money),
        walletId: cmd.walletId,
        playerId: cmd.playerId,
        roundId: cmd.roundId,
        gameId: cmd.gameId,
        kind: cmd.kind,
        money,
        referenceExternalTransactionId: cmd.referenceExternalTransactionId,
        createdAt: this.clock.now(),
      });
    } catch (err) {
      if (err instanceof DomainError) throw new ValidationError(err.message, { code: err.code });
      throw err;
    }
  }

  private async process(
    repos: TransactionalRepositories,
    candidate: WagerTransaction,
    opts: ProcessOptions,
  ): Promise<WagerResult> {
    const now = this.clock.now();

    if (opts.inbox) {
      const receipt = await repos.inbox.receive(
        InboxMessage.receive({ ...opts.inbox, receivedAt: now }),
      );
      if (receipt.duplicate) {
        if (receipt.existing.payloadHash !== opts.inbox.payloadHash) {
          throw new InboxConflictError(`message ${opts.inbox.messageId} was already consumed with another body`);
        }
        const original = await repos.transactions.findByIdempotencyKey(candidate.idempotencyKey);
        if (!original) throw new InboxConflictError(`message ${opts.inbox.messageId} consumed without a transaction`);
        return { transaction: original, idempotentReplay: true, duplicateMessage: true };
      }
    }

    // Fast path: replay without taking the wallet lock.
    const replay = await this.findReplay(repos, candidate);
    if (replay) return replay;

    const { wallet, contended } = await repos.wallets.lockForUpdate(candidate.walletId);
    if (contended) this.metrics.lockConflict("contended");
    if (!wallet) throw new NotFoundError("WALLET_NOT_FOUND", `wallet ${candidate.walletId} not found`);

    // Re-check under the lock: a concurrent duplicate may have committed while we waited.
    const lateReplay = await this.findReplay(repos, candidate);
    if (lateReplay) return lateReplay;

    const expectedVersion = wallet.version;
    const reference = candidate.referenceExternalTransactionId
      ? await repos.transactions.findByExternalId(candidate.providerId, candidate.referenceExternalTransactionId)
      : undefined;
    const referenceAlreadyReversed =
      reference && candidate.isReversal() ? await repos.transactions.hasProcessedReversal(reference.id) : false;

    const outcome = settleWagerTransaction(candidate, wallet, {
      reference,
      referenceAlreadyReversed,
      now,
      newEntryId: () => this.ids.next(),
      retryPolicy: this.retryPolicy,
    });

    await repos.transactions.insert(candidate);
    await this.persistOutcome(repos, candidate, wallet, expectedVersion, outcome, opts, now, true);
    return { transaction: candidate, idempotentReplay: false, duplicateMessage: false };
  }

  private async findReplay(
    repos: TransactionalRepositories,
    candidate: WagerTransaction,
  ): Promise<WagerResult | undefined> {
    const existing = await repos.transactions.findByIdempotencyKey(candidate.idempotencyKey);
    if (existing) {
      if (!existing.matchesPayload(candidate.payloadHash)) {
        this.metrics.duplicateDetected("idempotency_conflict");
        throw new IdempotencyConflictError(
          `idempotency key "${candidate.idempotencyKey}" was already used with a different payload`,
          { transactionId: existing.id },
        );
      }
      return { transaction: existing, idempotentReplay: true, duplicateMessage: false };
    }
    const sameExternal = await repos.transactions.findByExternalId(candidate.providerId, candidate.externalTransactionId);
    if (sameExternal) {
      this.metrics.duplicateDetected("idempotency_conflict");
      throw new IdempotencyConflictError(
        `transaction ${candidate.providerId}/${candidate.externalTransactionId} was already submitted under another idempotency key`,
        { transactionId: sameExternal.id },
      );
    }
    return undefined;
  }

  private report(result: WagerResult, opts: ProcessOptions, started: number): void {
    const tx = result.transaction;
    const fields = {
      transactionId: tx.id,
      walletId: tx.walletId,
      providerId: tx.providerId,
      kind: tx.kind,
      status: tx.status,
      failureCode: tx.failureCode,
      idempotentReplay: result.idempotentReplay,
    };
    if (result.duplicateMessage) {
      this.metrics.duplicateDetected("inbox_redelivery");
      this.logger.info("duplicate message ignored (inbox)", fields);
    } else if (result.idempotentReplay) {
      this.metrics.duplicateDetected("idempotent_replay");
      this.logger.info("idempotent replay", fields);
    } else {
      this.metrics.transactionFinished(tx.kind, tx.status, opts.source);
      this.logger.info("wager transaction settled", fields);
    }
    this.metrics.processingLatency(opts.source, (performance.now() - started) / 1000);
  }

  /**
   * Writes the consequences of a settlement: ledger + wallet (only when the balance moved),
   * wake-up of transactions waiting for this one, and the outbox events. Shared with the
   * pending-reference resolver so both paths emit identical events.
   */
  async persistOutcome(
    repos: TransactionalRepositories,
    tx: WagerTransaction,
    wallet: Wallet,
    expectedVersion: number,
    outcome: SettlementOutcome,
    opts: Pick<ProcessOptions, "correlationId" | "causationId">,
    now: Date,
    firstSettlement: boolean,
  ): Promise<void> {
    const events: IntegrationEvent<unknown>[] = [];
    const ctx = (): EventContext => ({
      eventId: this.ids.next(),
      correlationId: opts.correlationId,
      causationId: opts.causationId,
      occurredAt: now,
    });

    switch (outcome.type) {
      case "processed":
        if (outcome.entry) {
          await repos.ledger.insert(outcome.entry);
          await repos.wallets.saveBalance(wallet, expectedVersion);
          events.push(WalletBalanceChanged.from(wallet, outcome.entry, ctx()));
        }
        events.push(WagerTransactionProcessed.from(tx, ctx()));
        await repos.transactions.wakeDependents(tx.providerId, tx.externalTransactionId, now);
        break;
      case "rejected":
        events.push(WagerTransactionRejected.from(tx, ctx()));
        // A rejected reference must also unblock (and thus reject) whoever waits on it.
        await repos.transactions.wakeDependents(tx.providerId, tx.externalTransactionId, now);
        break;
      case "pending_reference":
        if (firstSettlement) events.push(WagerTransactionPendingReference.from(tx, ctx()));
        break;
    }
    if (events.length > 0) await repos.outbox.add(events.map((e) => OutboxMessage.enqueue(e)));
    if (tx.status === WagerTransactionStatus.Pending) throw new Error("settlement left transaction PENDING");
  }
}
