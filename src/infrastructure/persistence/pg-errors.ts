import { TransientError } from "../../application/errors";

export interface PgErrorInfo {
  code?: string;
  constraint?: string;
}

/** MikroORM copies the pg error's own properties (code = SQLSTATE, constraint) onto its exceptions. */
export function pgErrorInfo(err: unknown): PgErrorInfo | undefined {
  if (!err || typeof err !== "object") return undefined;
  const e = err as { code?: unknown; constraint?: unknown };
  return {
    code: typeof e.code === "string" ? e.code : undefined,
    constraint: typeof e.constraint === "string" ? e.constraint : undefined,
  };
}

const NETWORK_CODES = new Set(["ECONNREFUSED", "ECONNRESET", "ETIMEDOUT", "EPIPE", "ENOTFOUND", "EAI_AGAIN"]);

/**
 * Classifies infrastructure errors that are safe to retry: the transaction was rolled back (or
 * never started), so retrying cannot duplicate effects. Returns undefined for anything else.
 */
export function classifyTransient(err: unknown): TransientError | undefined {
  if (err instanceof TransientError) return err;
  const code = pgErrorInfo(err)?.code;
  const name = (err as Error | undefined)?.name ?? "";
  const message = (err as Error | undefined)?.message ?? "";
  let reason: string | undefined;
  if (code === "40P01" || code === "40001") reason = "deadlock";
  else if (code === "55P03") reason = "lock_timeout";
  else if (code === "57014") reason = "statement_timeout";
  else if (code === "53300" || code === "57P03") reason = "db_unavailable";
  else if (code?.startsWith("08") || code === "57P01" || code === "57P02") reason = "connection";
  else if (code && NETWORK_CODES.has(code)) reason = "connection";
  else if (name === "KnexTimeoutError" || /Timeout acquiring a connection/i.test(message)) reason = "pool_exhausted";
  else if (name === "ConnectionException") reason = "connection";
  else if (/Connection terminated|connect ECONNREFUSED|timeout exceeded when trying to connect|Client has encountered a connection error/i.test(message)) {
    reason = "connection";
  }
  return reason ? new TransientError(`database temporarily unavailable (${reason})`, reason, err) : undefined;
}
