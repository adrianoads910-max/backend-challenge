import { type ArgumentsHost, Catch, type ExceptionFilter, HttpException, Inject } from "@nestjs/common";
import type { Response } from "express";
import {
  type ApplicationError,
  ConcurrencyConflictError,
  DuplicateRaceError,
  IdempotencyConflictError,
  InboxConflictError,
  NotFoundError,
  TransientError,
  ValidationError,
  WalletAlreadyExistsError,
} from "../../application/errors";
import { type AppLogger, LOGGER } from "../../application/ports";
import { errorFields } from "../../infrastructure/observability/logger";
import { classifyTransient } from "../../infrastructure/persistence/pg-errors";

/**
 * One mapping for every endpoint:
 *   400 invalid payload · 404 unknown resource · 409 idempotency / uniqueness conflict
 *   422 business rejection (body of a REJECTED transaction) · 202 accepted, pending
 *   503 transient infrastructure failure (+ Retry-After) · 500 unexpected
 */
@Catch()
export class HttpErrorFilter implements ExceptionFilter {
  constructor(@Inject(LOGGER) private readonly logger: AppLogger) {}

  catch(exception: unknown, host: ArgumentsHost): void {
    const res = host.switchToHttp().getResponse<Response>();
    const err = classifyTransient(exception) ?? exception;
    const [status, code] = this.map(err);
    if (status === 503) res.setHeader("Retry-After", "1");
    if (status >= 500) this.logger.error("request failed", { status, ...errorFields(exception) });
    else this.logger.info("request refused", { status, code });

    if (err instanceof HttpException && !(status === 500)) {
      const message = err.message;
      res.status(status).json({ error: { code, message } });
      return;
    }
    const appErr = err as Partial<ApplicationError>;
    res.status(status).json({
      error: {
        code,
        message: status === 500 ? "internal error" : (appErr.message ?? "error"),
        ...(appErr.details && status < 500 ? { details: appErr.details } : {}),
      },
    });
  }

  private map(err: unknown): [number, string] {
    if (err instanceof ValidationError) return [400, err.code];
    if (err instanceof NotFoundError) return [404, err.code];
    if (err instanceof IdempotencyConflictError || err instanceof InboxConflictError || err instanceof WalletAlreadyExistsError) {
      return [409, err.code];
    }
    if (err instanceof TransientError) return [503, err.code];
    if (err instanceof ConcurrencyConflictError || err instanceof DuplicateRaceError) return [503, "TRANSIENT_FAILURE"];
    if (err instanceof HttpException) {
      const status = err.getStatus();
      if (status === 400) return [400, "VALIDATION_ERROR"];
      if (status === 404) return [404, "NOT_FOUND"];
      if (status === 401 || status === 403) return [status, status === 401 ? "UNAUTHENTICATED" : "FORBIDDEN"];
      return [status, "HTTP_ERROR"];
    }
    return [500, "INTERNAL_ERROR"];
  }
}
