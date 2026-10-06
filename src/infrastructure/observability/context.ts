import { AsyncLocalStorage } from "node:async_hooks";

/** Correlation fields attached to every log line emitted while handling one request/message. */
export interface LogContext {
  correlationId?: string;
  messageId?: string;
  transactionId?: string;
  walletId?: string;
  providerId?: string;
}

const storage = new AsyncLocalStorage<LogContext>();

export function runWithContext<T>(ctx: LogContext, fn: () => T): T {
  return storage.run({ ...storage.getStore(), ...ctx }, fn);
}

export function currentContext(): LogContext {
  return storage.getStore() ?? {};
}
