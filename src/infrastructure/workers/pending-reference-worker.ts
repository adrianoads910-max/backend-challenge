import { TransientError } from "../../application/errors";
import type { AppLogger } from "../../application/ports";
import type { ResolvePendingReference } from "../../application/resolve-pending-reference";
import type { AppConfig } from "../../config";
import { runWithContext } from "../observability/context";
import { errorFields } from "../observability/logger";
import { classifyTransient } from "../persistence/pg-errors";

/**
 * Scheduled resolution of out-of-order references. Every instance runs it; correctness under
 * concurrency comes from the wallet lock + re-check inside ResolvePendingReference, and the
 * exponential backoff (next_attempt_at) lives in the database, so restarts lose nothing.
 */
export class PendingReferenceWorker {
  private running = false;
  private loop?: Promise<void>;
  private timer?: ReturnType<typeof setTimeout>;
  private wake?: () => void;

  constructor(
    private readonly resolver: ResolvePendingReference,
    private readonly config: AppConfig,
    private readonly logger: AppLogger,
  ) {}

  start(): void {
    if (this.running) return;
    this.running = true;
    this.loop = this.run();
    this.logger.info("pending reference worker started");
  }

  async stop(): Promise<void> {
    this.running = false;
    clearTimeout(this.timer);
    this.wake?.();
    await this.loop;
  }

  private async run(): Promise<void> {
    while (this.running) {
      try {
        await this.tick();
      } catch (err) {
        this.logger.warn("pending reference scan failed", errorFields(err));
      }
      await new Promise<void>((resolve) => {
        this.wake = resolve;
        this.timer = setTimeout(resolve, this.config.PENDING_REFERENCE_POLL_MS);
      });
    }
  }

  async tick(): Promise<void> {
    const ids = await this.resolver.dueIds(50);
    for (const id of ids) {
      if (!this.running) return;
      const correlationId = `pending-reference:${id}`;
      await runWithContext({ correlationId, transactionId: id }, async () => {
        try {
          await this.resolver.execute(id, correlationId);
        } catch (err) {
          if (err instanceof TransientError || classifyTransient(err)) {
            this.logger.warn("pending reference resolution deferred", errorFields(err));
            return;
          }
          this.logger.error("pending reference resolution crashed", errorFields(err));
          await this.resolver.markFailed(id).catch((e) => this.logger.error("could not mark FAILED", errorFields(e)));
        }
      });
    }
  }
}
