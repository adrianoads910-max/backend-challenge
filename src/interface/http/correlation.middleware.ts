import { randomUUID } from "node:crypto";
import type { NextFunction, Request, Response } from "express";
import { runWithContext } from "../../infrastructure/observability/context";

const VALID = /^[A-Za-z0-9._:\-]{1,128}$/;

/** Accepts X-Correlation-Id (or generates one), echoes it back and binds it to the log context. */
export function correlationMiddleware(req: Request, res: Response, next: NextFunction): void {
  const incoming = req.header("x-correlation-id");
  const correlationId = incoming && VALID.test(incoming) ? incoming : randomUUID();
  res.setHeader("X-Correlation-Id", correlationId);
  runWithContext({ correlationId }, () => next());
}
