import { SendMessageBatchCommand, type SQSClient } from "@aws-sdk/client-sqs";
import type { MikroORM } from "@mikro-orm/postgresql";
import type { AppLogger } from "../../application/ports";
import type { AppConfig } from "../../config";
import { OutboxMessage } from "../../domain/outbox-message";
import { errorFields } from "../observability/logger";
import type { PromMetrics } from "../observability/metrics";

interface ClaimedRow {
  id: string;
  aggregate_id: string;
  event_type: string;
  payload: Record<string, unknown>;
  occurred_at: Date;
  attempts: number;
  next_attempt_at: Date | null;
}

/**
 * Relays committed outbox rows to the events queue. Safe with any number of concurrent publishers:
 *
 *  1. claim: a short transaction leases a batch (`FOR UPDATE SKIP LOCKED` + locked_until), so two
 *     publishers never pick the same row at the same time and no DB transaction stays open while
 *     talking to SQS;
 *  2. publish to SQS (FIFO: group = aggregate, dedup id = eventId);
 *  3. mark published only if we still own the lease.
 *
 * If the process dies between 2 and 3 the lease expires and another instance republishes: delivery
 * is at-least-once, and consumers dedup by eventId (the FIFO dedup window also absorbs most repeats).
 */
export class OutboxPublisher {
  private running = false;
  private loop?: Promise<void>;
  private wake?: () => void;
  private readonly fifo: boolean;

  constructor(
    private readonly orm: MikroORM,
    private readonly sqs: SQSClient,
    private readonly queueUrl: string,
    private readonly config: AppConfig,
    private readonly logger: AppLogger,
    private readonly metrics: PromMetrics,
    private readonly instanceId: string,
  ) {
    this.fifo = queueUrl.endsWith(".fifo");
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.loop = this.run();
    this.logger.info("outbox publisher started");
  }

  async stop(): Promise<void> {
    this.running = false;
    this.wake?.();
    await this.loop;
  }

  private async run(): Promise<void> {
    while (this.running) {
      let published = 0;
      try {
        published = await this.publishBatch();
      } catch (err) {
        this.logger.warn("outbox publish cycle failed", errorFields(err));
      }
      // Drain quickly while there is backlog, otherwise poll.
      if (published === 0 && this.running) {
        await new Promise<void>((resolve) => {
          const t = setTimeout(resolve, this.config.OUTBOX_POLL_MS);
          this.wake = () => {
            clearTimeout(t);
            resolve();
          };
        });
      }
    }
  }

  /** One claim → publish → mark cycle. Returns how many events were published. */
  async publishBatch(): Promise<number> {
    const rows = await this.claim();
    if (rows.length === 0) return 0;

    const published: string[] = [];
    const failed = new Map<string, string>();
    // SQS batches hold at most 10 entries; send all chunks of the claim concurrently.
    const chunks: ClaimedRow[][] = [];
    for (let i = 0; i < rows.length; i += 10) chunks.push(rows.slice(i, i + 10));
    await Promise.all(
      chunks.map(async (chunk) => {
        try {
          const res = await this.sqs.send(
            new SendMessageBatchCommand({
              QueueUrl: this.queueUrl,
              Entries: chunk.map((row, idx) => ({
                Id: String(idx),
                MessageBody: JSON.stringify(row.payload),
                ...(this.fifo ? { MessageGroupId: row.aggregate_id, MessageDeduplicationId: row.id } : {}),
                MessageAttributes: {
                  eventType: { DataType: "String", StringValue: row.event_type },
                  eventId: { DataType: "String", StringValue: row.id },
                },
              })),
            }),
          );
          for (const ok of res.Successful ?? []) published.push(chunk[Number(ok.Id)]!.id);
          for (const ko of res.Failed ?? []) failed.set(chunk[Number(ko.Id)]!.id, ko.Message ?? ko.Code ?? "failed");
        } catch (err) {
          for (const row of chunk) failed.set(row.id, (err as Error).message ?? "publish failed");
        }
      }),
    );

    this.faultInjection("crash_after_publish_before_mark");
    await this.markPublished(published);
    await this.reschedule(rows.filter((r) => failed.has(r.id)), failed);

    for (const row of rows) if (published.includes(row.id)) this.metrics.outboxPublished.inc({ event_type: row.event_type });
    if (failed.size) {
      this.metrics.outboxFailures.inc(failed.size);
      this.logger.warn("outbox events rescheduled", { failed: failed.size });
    }
    return published.length;
  }

  private async claim(): Promise<ClaimedRow[]> {
    const em = this.orm.em.fork();
    return em.execute<ClaimedRow[]>(
      `update outbox_messages o
          set locked_by = ?, locked_until = now() + make_interval(secs => ? / 1000.0)
        where o.id in (
          select id from outbox_messages
           where published_at is null
             and (next_attempt_at is null or next_attempt_at <= now())
             and (locked_until is null or locked_until < now())
           order by seq
           limit ?
           for update skip locked)
        returning o.id, o.aggregate_id, o.event_type, o.payload, o.occurred_at, o.attempts, o.next_attempt_at`,
      [this.instanceId, this.config.OUTBOX_LEASE_MS, this.config.OUTBOX_BATCH_SIZE],
    );
  }

  private async markPublished(ids: string[]): Promise<void> {
    if (ids.length === 0) return;
    await this.orm.em.fork().execute(
      `update outbox_messages set published_at = now(), locked_by = null, locked_until = null, last_error = null
        where id = any(?::uuid[]) and locked_by = ? and published_at is null`,
      [`{${ids.join(",")}}`, this.instanceId],
    );
  }

  private async reschedule(rows: ClaimedRow[], errors: Map<string, string>): Promise<void> {
    const now = new Date();
    for (const row of rows) {
      const message = OutboxMessage.rehydrate({
        id: row.id,
        aggregateId: row.aggregate_id,
        eventType: row.event_type,
        payload: row.payload,
        occurredAt: row.occurred_at,
        attempts: row.attempts,
        nextAttemptAt: row.next_attempt_at ?? undefined,
      });
      message.scheduleRetry(now);
      await this.orm.em.fork().execute(
        `update outbox_messages set attempts = ?, next_attempt_at = ?, last_error = ?, locked_by = null, locked_until = null
          where id = ? and locked_by = ?`,
        [message.attempts, message.nextAttemptAt!, (errors.get(row.id) ?? "").slice(0, 500), row.id, this.instanceId],
      );
    }
  }

  private faultInjection(point: string): void {
    if (this.config.FAULT_INJECTION === point) {
      this.logger.error("fault injection: crashing", { point });
      process.kill(process.pid, "SIGKILL");
    }
  }
}
