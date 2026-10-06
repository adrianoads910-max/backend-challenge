/**
 * Application-level errors. Each maps to exactly one HTTP status and one queue disposition
 * (see interface/http/error-mapping.ts and infrastructure/messaging/wager-consumer.ts).
 */
export abstract class ApplicationError extends Error {
  abstract readonly code: string;
  constructor(message: string, public readonly details?: unknown) {
    super(message);
    this.name = new.target.name;
  }
}

/** Malformed or semantically invalid input. Never retryable as-is. */
export class ValidationError extends ApplicationError {
  readonly code = "VALIDATION_ERROR";
}

/** Same idempotency key (or same provider external id) with a different payload. */
export class IdempotencyConflictError extends ApplicationError {
  readonly code = "IDEMPOTENCY_CONFLICT";
}

/** Same (consumer, messageId) seen before with a different message body. */
export class InboxConflictError extends ApplicationError {
  readonly code = "INBOX_MESSAGE_CONFLICT";
}

export class WalletAlreadyExistsError extends ApplicationError {
  readonly code = "WALLET_ALREADY_EXISTS";
}

export class NotFoundError extends ApplicationError {
  constructor(public override readonly code: "WALLET_NOT_FOUND" | "TRANSACTION_NOT_FOUND", message: string) {
    super(message);
  }
}

/** Temporary infrastructure problem (DB down, lock timeout, deadlock…). Safe to retry. */
export class TransientError extends ApplicationError {
  readonly code = "TRANSIENT_FAILURE";
  constructor(message: string, public readonly reason: string, public override readonly cause?: unknown) {
    super(message);
  }
}

/** Optimistic version check failed despite the row lock. Treated as transient and retried. */
export class ConcurrencyConflictError extends ApplicationError {
  readonly code = "CONCURRENCY_CONFLICT";
}

/** A unique constraint fired for a race the use case knows how to resolve by re-reading. */
export class DuplicateRaceError extends ApplicationError {
  readonly code = "DUPLICATE_RACE";
  constructor(public readonly constraint: string) {
    super(`unique constraint ${constraint} raced`);
  }
}
