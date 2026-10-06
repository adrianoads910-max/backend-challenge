import { InvalidTransactionError, InvalidTransactionStateError } from "./errors";
import type { FailureCode } from "./failure-code";
import { LedgerDirection } from "./ledger-entry";
import type { Money } from "./money";

export enum WagerTransactionKind {
  Opening = "OPENING",
  Bet = "BET",
  Win = "WIN",
  Loss = "LOSS",
  Refund = "REFUND",
  Rollback = "ROLLBACK",
}

export enum WagerTransactionStatus {
  Pending = "PENDING",
  PendingReference = "PENDING_REFERENCE",
  Processed = "PROCESSED",
  Rejected = "REJECTED",
  Failed = "FAILED",
}

/** Kinds a provider may submit (API or queue). OPENING is internal only. */
export const SUBMITTABLE_KINDS = [
  WagerTransactionKind.Bet,
  WagerTransactionKind.Win,
  WagerTransactionKind.Loss,
  WagerTransactionKind.Refund,
  WagerTransactionKind.Rollback,
] as const;

/** Which kinds each kind may reference (business rule 3; WIN → BET is optional). */
const ALLOWED_REFERENCE_KINDS: Partial<Record<WagerTransactionKind, readonly WagerTransactionKind[]>> = {
  [WagerTransactionKind.Win]: [WagerTransactionKind.Bet],
  [WagerTransactionKind.Loss]: [WagerTransactionKind.Bet],
  [WagerTransactionKind.Refund]: [WagerTransactionKind.Bet],
  [WagerTransactionKind.Rollback]: [WagerTransactionKind.Bet, WagerTransactionKind.Win, WagerTransactionKind.Refund],
};

const TERMINAL = new Set([
  WagerTransactionStatus.Processed,
  WagerTransactionStatus.Rejected,
  WagerTransactionStatus.Failed,
]);

export interface CreateWagerTransactionProps {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: Money;
  referenceExternalTransactionId?: string;
  createdAt: Date;
}

export interface WagerTransactionState extends CreateWagerTransactionProps {
  status: WagerTransactionStatus;
  referenceTransactionId?: string;
  failureCode?: FailureCode;
  processedAt?: Date;
  /** Wallet balance observed when the transaction reached its current status (replay result). */
  resultBalance?: Money;
  referenceAttempts: number;
  nextAttemptAt?: Date;
}

/** Exponential backoff for PENDING_REFERENCE resolution. */
export interface ReferenceRetryPolicy {
  maxAttempts: number;
  baseDelayMs: number;
  maxDelayMs: number;
}

/**
 * Transitions (anything else throws InvalidTransactionStateError):
 *
 *   PENDING ──────────► PROCESSED | REJECTED | FAILED
 *      │
 *      └──► PENDING_REFERENCE ──► PENDING_REFERENCE (retry scheduled)
 *                           └───► PROCESSED | REJECTED | FAILED
 *
 * PROCESSED, REJECTED and FAILED are terminal. The database enforces the same rule with a trigger.
 */
export class WagerTransaction {
  private constructor(
    public readonly id: string,
    public readonly providerId: string,
    public readonly externalTransactionId: string,
    public readonly idempotencyKey: string,
    public readonly payloadHash: string,
    public readonly walletId: string,
    public readonly playerId: string,
    public readonly roundId: string,
    public readonly gameId: string,
    public readonly kind: WagerTransactionKind,
    public readonly money: Money,
    public readonly referenceExternalTransactionId: string | undefined,
    public readonly createdAt: Date,
    private _status: WagerTransactionStatus,
    private _referenceTransactionId?: string,
    private _failureCode?: FailureCode,
    private _processedAt?: Date,
    private _resultBalance?: Money,
    private _referenceAttempts = 0,
    private _nextAttemptAt?: Date,
  ) {}

  /** Born PENDING. Validates per-kind requirements of a provider-submitted transaction. */
  static create(props: CreateWagerTransactionProps): WagerTransaction {
    if (props.kind === WagerTransactionKind.Opening) {
      throw new InvalidTransactionError("OPENING is internal and cannot be submitted");
    }
    if (!SUBMITTABLE_KINDS.includes(props.kind as (typeof SUBMITTABLE_KINDS)[number])) {
      throw new InvalidTransactionError(`unknown kind ${props.kind}`);
    }
    return WagerTransaction.build(props);
  }

  /** Internal credit booked when a wallet is opened with a positive balance. */
  static createOpening(props: Omit<CreateWagerTransactionProps, "kind" | "referenceExternalTransactionId">) {
    return WagerTransaction.build({ ...props, kind: WagerTransactionKind.Opening });
  }

  private static build(props: CreateWagerTransactionProps): WagerTransaction {
    const tx = new WagerTransaction(
      props.id,
      props.providerId,
      props.externalTransactionId,
      props.idempotencyKey,
      props.payloadHash,
      props.walletId,
      props.playerId,
      props.roundId,
      props.gameId,
      props.kind,
      props.money,
      props.referenceExternalTransactionId || undefined,
      props.createdAt,
      WagerTransactionStatus.Pending,
    );
    if (tx.requiresReference() && !tx.referenceExternalTransactionId) {
      throw new InvalidTransactionError(`${tx.kind} requires referenceExternalTransactionId`);
    }
    if (tx.kind === WagerTransactionKind.Opening && tx.referenceExternalTransactionId) {
      throw new InvalidTransactionError("OPENING cannot reference another transaction");
    }
    if (tx.referenceExternalTransactionId === tx.externalTransactionId) {
      throw new InvalidTransactionError("a transaction cannot reference itself");
    }
    if (tx.money.isNegative()) throw new InvalidTransactionError("amount must not be negative");
    if (tx.affectsBalance() && !tx.money.isPositive()) {
      throw new InvalidTransactionError(`${tx.kind} amount must be greater than zero`);
    }
    return tx;
  }

  static rehydrate(s: WagerTransactionState): WagerTransaction {
    return new WagerTransaction(
      s.id,
      s.providerId,
      s.externalTransactionId,
      s.idempotencyKey,
      s.payloadHash,
      s.walletId,
      s.playerId,
      s.roundId,
      s.gameId,
      s.kind,
      s.money,
      s.referenceExternalTransactionId,
      s.createdAt,
      s.status,
      s.referenceTransactionId,
      s.failureCode,
      s.processedAt,
      s.resultBalance,
      s.referenceAttempts,
      s.nextAttemptAt,
    );
  }

  get status(): WagerTransactionStatus {
    return this._status;
  }
  get referenceTransactionId(): string | undefined {
    return this._referenceTransactionId;
  }
  get failureCode(): FailureCode | undefined {
    return this._failureCode;
  }
  get processedAt(): Date | undefined {
    return this._processedAt;
  }
  get resultBalance(): Money | undefined {
    return this._resultBalance;
  }
  get referenceAttempts(): number {
    return this._referenceAttempts;
  }
  get nextAttemptAt(): Date | undefined {
    return this._nextAttemptAt;
  }

  // ---------------------------------------------------------------- transitions

  markProcessed(referenceTransactionId: string | undefined, at: Date, balance: Money): void {
    this.assertNotTerminal("markProcessed");
    this._status = WagerTransactionStatus.Processed;
    this._referenceTransactionId = referenceTransactionId;
    this._processedAt = at;
    this._resultBalance = balance;
    this._nextAttemptAt = undefined;
  }

  /**
   * Parks the transaction waiting for its reference and schedules the next resolution attempt.
   * Returns false when the retry budget is exhausted (caller must then reject it).
   */
  markPendingReference(now: Date, policy: ReferenceRetryPolicy, balance: Money): boolean {
    this.assertNotTerminal("markPendingReference");
    if (this._status === WagerTransactionStatus.PendingReference && this._referenceAttempts >= policy.maxAttempts) {
      return false;
    }
    const attempt = this._status === WagerTransactionStatus.PendingReference ? this._referenceAttempts + 1 : 0;
    const delay = Math.min(policy.baseDelayMs * 2 ** attempt, policy.maxDelayMs);
    this._status = WagerTransactionStatus.PendingReference;
    this._referenceAttempts = attempt;
    this._nextAttemptAt = new Date(now.getTime() + delay);
    this._resultBalance = balance;
    return true;
  }

  reject(code: FailureCode, at: Date, balance: Money): void {
    this.assertNotTerminal("reject");
    this._status = WagerTransactionStatus.Rejected;
    this._failureCode = code;
    this._processedAt = at;
    this._resultBalance = balance;
    this._nextAttemptAt = undefined;
  }

  fail(code: FailureCode, at: Date): void {
    this.assertNotTerminal("fail");
    this._status = WagerTransactionStatus.Failed;
    this._failureCode = code;
    this._processedAt = at;
    this._nextAttemptAt = undefined;
  }

  // ---------------------------------------------------------------- queries

  isTerminal(): boolean {
    return TERMINAL.has(this._status);
  }

  affectsBalance(): boolean {
    return this.kind !== WagerTransactionKind.Loss;
  }

  requiresReference(): boolean {
    return this.kind === WagerTransactionKind.Refund || this.kind === WagerTransactionKind.Rollback;
  }

  isReversal(): boolean {
    return this.requiresReference();
  }

  matchesPayload(payloadHash: string): boolean {
    return this.payloadHash === payloadHash;
  }

  canReference(kind: WagerTransactionKind): boolean {
    return ALLOWED_REFERENCE_KINDS[this.kind]?.includes(kind) ?? false;
  }

  /** Same provider, player, wallet, currency and round (business rule 2). */
  sharesScopeWith(reference: WagerTransaction): boolean {
    return (
      reference.providerId === this.providerId &&
      reference.playerId === this.playerId &&
      reference.walletId === this.walletId &&
      reference.money.currency === this.money.currency &&
      reference.roundId === this.roundId
    );
  }

  ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection {
    switch (this.kind) {
      case WagerTransactionKind.Bet:
        return LedgerDirection.Debit;
      case WagerTransactionKind.Opening:
      case WagerTransactionKind.Win:
      case WagerTransactionKind.Refund:
        return LedgerDirection.Credit;
      case WagerTransactionKind.Rollback: {
        if (!reference) throw new InvalidTransactionError("ROLLBACK direction depends on its reference");
        const original = reference.ledgerDirectionFor();
        return original === LedgerDirection.Debit ? LedgerDirection.Credit : LedgerDirection.Debit;
      }
      case WagerTransactionKind.Loss:
        throw new InvalidTransactionError("LOSS does not move the balance");
    }
  }

  private assertNotTerminal(transition: string): void {
    if (this.isTerminal()) {
      throw new InvalidTransactionStateError(`cannot ${transition}: transaction ${this.id} is ${this._status}`);
    }
  }
}
