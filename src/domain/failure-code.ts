/**
 * Stable, machine-readable reasons attached to REJECTED / FAILED transactions.
 * The provider decides from the code alone whether to resend, fix the payload or give up.
 * See ARCHITECTURE.md §"Failure codes" for the full taxonomy.
 */
export enum FailureCode {
  // --- business rejections (terminal, resending the same payload yields the same answer)
  InsufficientFunds = "INSUFFICIENT_FUNDS",
  ReversalInsufficientFunds = "REVERSAL_INSUFFICIENT_FUNDS",
  CurrencyMismatch = "CURRENCY_MISMATCH",
  WalletPlayerMismatch = "WALLET_PLAYER_MISMATCH",
  ReferenceNotFound = "REFERENCE_NOT_FOUND",
  ReferenceScopeMismatch = "REFERENCE_SCOPE_MISMATCH",
  ReferenceKindNotAllowed = "REFERENCE_KIND_NOT_ALLOWED",
  ReferenceNotProcessed = "REFERENCE_NOT_PROCESSED",
  ReferenceAlreadyReversed = "REFERENCE_ALREADY_REVERSED",
  ReversalAmountMismatch = "REVERSAL_AMOUNT_MISMATCH",
  // --- permanent infrastructure failures (terminal, auditable, need an operator)
  ProcessingFailed = "PROCESSING_FAILED",
}
