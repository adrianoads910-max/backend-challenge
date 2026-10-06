import { GetQueueAttributesCommand, type SQSClient } from "@aws-sdk/client-sqs";
import { Controller, Get, Inject, Res } from "@nestjs/common";
import type { MikroORM } from "@mikro-orm/postgresql";
import type { Response } from "express";
import { Public } from "../../infrastructure/auth/provider-auth.guard";
import type { QueueUrls } from "../../infrastructure/messaging/sqs";
import type { PromMetrics } from "../../infrastructure/observability/metrics";

export const ORM = Symbol("MikroORM");
export const SQS_CLIENT = Symbol("SQSClient");
export const QUEUE_URLS = Symbol("QueueUrls");
export const PROM_METRICS = Symbol("PromMetrics");

const withTimeout = <T>(p: Promise<T>, ms: number) =>
  Promise.race([p, new Promise<never>((_, reject) => setTimeout(() => reject(new Error(`timeout after ${ms}ms`)), ms))]);

@Public()
@Controller()
export class HealthController {
  constructor(
    @Inject(ORM) private readonly orm: MikroORM,
    @Inject(SQS_CLIENT) private readonly sqs: SQSClient,
    @Inject(QUEUE_URLS) private readonly queues: QueueUrls,
    @Inject(PROM_METRICS) private readonly metrics: PromMetrics,
  ) {}

  /** Liveness: the process and its event loop respond. No dependency checks (avoids restart storms). */
  @Get("health/live")
  live() {
    return { status: "ok" };
  }

  /** Readiness: PostgreSQL and SQS reachable. */
  @Get("health/ready")
  async ready(@Res({ passthrough: true }) res: Response) {
    const check = async (fn: () => Promise<unknown>) => {
      try {
        await withTimeout(fn(), 2_000);
        return { status: "up" as const };
      } catch (err) {
        return { status: "down" as const, error: (err as Error).message.split("\n")[0] };
      }
    };
    const [postgres, sqs] = await Promise.all([
      check(() => this.orm.em.fork().execute("select 1")),
      check(() => this.sqs.send(new GetQueueAttributesCommand({ QueueUrl: this.queues.wager, AttributeNames: ["QueueArn"] }))),
    ]);
    const ok = postgres.status === "up" && sqs.status === "up";
    res.status(ok ? 200 : 503);
    return { status: ok ? "ok" : "unavailable", checks: { postgres, sqs } };
  }

  @Get("metrics")
  async metricsEndpoint(@Res() res: Response) {
    await this.refreshGauges();
    res.setHeader("Content-Type", this.metrics.registry.contentType);
    res.send(await this.metrics.registry.metrics());
  }

  private async refreshGauges(): Promise<void> {
    try {
      const [row] = await this.orm.em.fork().execute<{ pending: string; lag: string | null; refs: string }[]>(
        `select (select count(*) from outbox_messages where published_at is null)::text as pending,
                (select extract(epoch from now() - min(occurred_at)) from outbox_messages where published_at is null)::text as lag,
                (select count(*) from wager_transactions where status = 'PENDING_REFERENCE')::text as refs`,
      );
      this.metrics.outboxPending.set(Number(row!.pending));
      this.metrics.outboxLag.set(row!.lag ? Number(row!.lag) : 0);
      this.metrics.pendingReferences.set(Number(row!.refs));
    } catch {
      /* best effort */
    }
    try {
      const attrs = await this.sqs.send(
        new GetQueueAttributesCommand({ QueueUrl: this.queues.dlq, AttributeNames: ["ApproximateNumberOfMessages"] }),
      );
      this.metrics.dlqDepth.set(Number(attrs.Attributes?.ApproximateNumberOfMessages ?? 0));
    } catch {
      /* best effort */
    }
  }
}
