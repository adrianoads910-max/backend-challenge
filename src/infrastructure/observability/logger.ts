import type { LoggerService } from "@nestjs/common";
import pino, { type Logger } from "pino";
import type { AppLogger } from "../../application/ports";
import { currentContext } from "./context";

/**
 * Structured JSON logs. Correlation fields come from the AsyncLocalStorage context; anything that
 * could carry a full financial payload or credentials is redacted defensively.
 */
export class PinoAppLogger implements AppLogger, LoggerService {
  private readonly pino: Logger;

  constructor(level: string, base: Record<string, unknown>) {
    this.pino = pino({
      level,
      base,
      messageKey: "message",
      timestamp: pino.stdTimeFunctions.isoTime,
      formatters: { level: (label) => ({ level: label }) },
      mixin: () => ({ ...currentContext() }),
      redact: {
        paths: ["payload", "body", "data", "money", "*.payload", "*.body", "*.money", "headers.authorization", "authorization"],
        censor: "[redacted]",
      },
    });
  }

  info(msg: string, fields?: Record<string, unknown>) {
    this.pino.info(fields ?? {}, msg);
  }
  warn(msg: string, fields?: Record<string, unknown>) {
    this.pino.warn(fields ?? {}, msg);
  }
  error(msg: string, fields?: Record<string, unknown>) {
    this.pino.error(fields ?? {}, msg);
  }
  debug(msg: string, fields?: Record<string, unknown>) {
    this.pino.debug(fields ?? {}, msg);
  }

  // ---- NestJS LoggerService (framework messages)
  log(message: unknown, context?: string) {
    this.pino.info({ context }, String(message));
  }
  verbose(message: unknown, context?: string) {
    this.pino.trace({ context }, String(message));
  }
  fatal(message: unknown, context?: string) {
    this.pino.fatal({ context }, String(message));
  }
}

export function errorFields(err: unknown): Record<string, unknown> {
  if (err instanceof Error) {
    const code = (err as { code?: unknown }).code;
    return { err: { type: err.name, message: err.message, ...(code ? { code } : {}) } };
  }
  return { err: { message: String(err) } };
}
