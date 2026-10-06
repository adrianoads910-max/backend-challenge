/** Base for every error raised by the domain layer. Never carries infrastructure details. */
export abstract class DomainError extends Error {
  abstract readonly code: string;
  constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

/** Input that can never become a valid value object (bad amount, bad currency…). */
export class InvalidMoneyError extends DomainError {
  readonly code = "INVALID_MONEY";
}

export class CurrencyMismatchError extends DomainError {
  readonly code = "CURRENCY_MISMATCH";
}

/** Arithmetic or state that would break an aggregate invariant. */
export class InvariantViolationError extends DomainError {
  readonly code = "INVARIANT_VIOLATION";
}

export class InsufficientFundsError extends DomainError {
  readonly code = "INSUFFICIENT_FUNDS";
}

/** Programming error: attempt to move a transaction out of a terminal state. */
export class InvalidTransactionStateError extends DomainError {
  readonly code = "INVALID_TRANSACTION_STATE";
}

export class InvalidTransactionError extends DomainError {
  readonly code = "INVALID_TRANSACTION";
}
