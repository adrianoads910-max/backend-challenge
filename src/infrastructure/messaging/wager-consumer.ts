import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  type Message,
  ReceiveMessageCommand,
  SendMessageCommand,
  type SQSClient,
} from "@aws-sdk/client-sqs";
import {
  ApplicationError,
  IdempotencyConflictError,
  InboxConflictError,
  NotFoundError,
  TransientError,
  ValidationError,
} from "../../application/errors";
import { sha256Canonical } from "../../application/payload-hash";
import type { ProcessWagerTransaction } from "../../application/process-wager-transaction";
import type { AppLogger } from "../../application/ports";
import type { AppConfig } from "../../config";
import { parse, toCommand, wagerMessage } from "../../interface/contracts";
import { runWithContext } from "../observability/context";
import { errorFields } from "../observability/logger";
import type { PromMetrics } from "../observability/metrics";
import { classifyTransient } from "../persistence/pg-errors";
import type { QueueUrls } from "./sqs";

export const CONSUMER_NAME = "wager-transactions-consumer";

type Disposition =
  | { type: "ack"; outcome: "acked" | "duplicate" }
  | { type: "retry"; reason: string }
  | { type: "dead_letter"; reason: string; code: string };

/**
 * Long-polls wager-transactions.fifo and feeds the same use case as the HTTP API.
 *
 *  - business outcome (PROCESSED / REJECTED / PENDING_REFERENCE) → commit, then ack (delete)
 *  - transient failure (DB down, lock timeout…) → visibility backoff, retried by SQS
 *  - permanent failure (malformed, idempotency conflict, unknown wallet) or too many receives → DLQ
 *
 * The ack happens strictly after the commit. A crash in between leads to a redelivery that the
 * persistent inbox turns into a no-op.
 */
export class WagerConsumer {
  private running = false;
  private loop?: Promise<void>;
  private readonly inFlight = new Set<Promise<void>>();
  private abort = new AbortController();

  constructor(
    private readonly sqs: SQSClient,
    private readonly queues: QueueUrls,
    private readonly processor: ProcessWagerTransaction,
    private readonly config: AppConfig,
    private readonly logger: AppLogger,
    private readonly metrics: PromMetrics,
  ) {}

  start(): void {
    if (this.running) return;
    this.running = true;
    this.abort = new AbortController();
    this.loop = this.poll();
    this.logger.info("wager consumer started", { queue: this.config.SQS_WAGER_QUEUE });
  }

  /** SIGTERM: stop receiving, finish what is in progress, hand back what was not started. */
  async stop(): Promise<void> {
    if (!this.running) return;
    this.running = false;
    this.abort.abort();
    await this.loop?.catch(() => undefined);
    await Promise.allSettled([...this.inFlight]);
    this.logger.info("wager consumer stopped");
  }

  private async poll(): Promise<void> {
    while (this.running) {
      let messages: Message[] = [];
      try {
        const res = await this.sqs.send(
          new ReceiveMessageCommand({
            QueueUrl: this.queues.wager,
            MaxNumberOfMessages: 10,
            WaitTimeSeconds: this.config.CONSUMER_WAIT_TIME_S,
            VisibilityTimeout: this.config.CONSUMER_VISIBILITY_TIMEOUT_S,
            MessageSystemAttributeNames: ["ApproximateReceiveCount", "MessageGroupId"],
          }),
          { abortSignal: this.abort.signal },
        );
        messages = res.Messages ?? [];
      } catch (err) {
        if (!this.running) break;
        this.logger.warn("sqs receive failed", errorFields(err));
        await sleep(1_000);
        continue;
      }
      if (messages.length === 0) continue;
      if (!this.running) {
        await this.release(messages);
        break;
      }
      const batch = this.handleBatch(messages);
      this.inFlight.add(batch);
      await batch.finally(() => this.inFlight.delete(batch));
    }
  }

  /**
   * FIFO semantics: messages of the same group (= wallet) are handled sequentially and in order;
   * different groups run in parallel. If one message of a group is not acked, the rest of that
   * group in the batch is released so ordering is preserved on redelivery.
   */
  private async handleBatch(messages: Message[]): Promise<void> {
    const groups = new Map<string, Message[]>();
    for (const m of messages) {
      const group = m.Attributes?.MessageGroupId ?? m.MessageId!;
      groups.set(group, [...(groups.get(group) ?? []), m]);
    }
    await Promise.all(
      [...groups.values()].map(async (group) => {
        for (let i = 0; i < group.length; i++) {
          if (!this.running) {
            await this.release(group.slice(i));
            return;
          }
          const acked = await this.handle(group[i]!);
          if (!acked) {
            await this.release(group.slice(i + 1));
            return;
          }
        }
      }),
    );
  }

  /** Returns true when the message left the queue (acked or dead-lettered). */
  private async handle(message: Message): Promise<boolean> {
    const receiveCount = Number.parseInt(message.Attributes?.ApproximateReceiveCount ?? "1", 10);
    let envelopeId = message.MessageId!;
    let disposition: Disposition;
    try {
      const raw = JSON.parse(message.Body ?? "");
      const envelope = parse(wagerMessage, raw);
      envelopeId = envelope.messageId;
      disposition = await runWithContext(
        {
          correlationId: envelope.messageId,
          messageId: envelope.messageId,
          walletId: envelope.data.walletId,
          providerId: envelope.data.providerId,
        },
        async () => {
          const { idempotencyKey, ...body } = envelope.data;
          const result = await this.processor.execute(toCommand(body, idempotencyKey), {
            source: "sqs",
            correlationId: envelope.messageId,
            causationId: envelope.messageId,
            inbox: {
              consumerName: CONSUMER_NAME,
              messageId: envelope.messageId,
              payloadHash: sha256Canonical({ type: envelope.type, data: envelope.data }),
            },
          });
          this.faultInjection("crash_after_commit_before_ack");
          return { type: "ack", outcome: result.duplicateMessage ? "duplicate" : "acked" } as const;
        },
      );
    } catch (err) {
      disposition = this.classify(err, receiveCount);
      const level = disposition.type === "retry" ? "warn" : "error";
      this.logger[level]("wager message not processed", {
        messageId: envelopeId,
        disposition: disposition.type,
        receiveCount,
        ...errorFields(err),
      });
    }
    return runWithContext({ messageId: envelopeId }, () => this.apply(message, disposition, receiveCount));
  }

  private classify(err: unknown, receiveCount: number): Disposition {
    if (err instanceof SyntaxError) return { type: "dead_letter", reason: "malformed_json", code: "MALFORMED_MESSAGE" };
    if (
      err instanceof ValidationError ||
      err instanceof IdempotencyConflictError ||
      err instanceof InboxConflictError ||
      err instanceof NotFoundError
    ) {
      return { type: "dead_letter", reason: "permanent", code: (err as ApplicationError).code };
    }
    const transient = err instanceof TransientError ? err : classifyTransient(err);
    if (receiveCount >= this.config.CONSUMER_MAX_RECEIVES) {
      return { type: "dead_letter", reason: "max_receives_exceeded", code: transient?.code ?? "UNEXPECTED_ERROR" };
    }
    // Unknown errors are retried too (bounded by CONSUMER_MAX_RECEIVES) rather than dropped.
    return { type: "retry", reason: transient?.reason ?? "unexpected" };
  }

  private async apply(message: Message, d: Disposition, receiveCount: number): Promise<boolean> {
    try {
      switch (d.type) {
        case "ack":
          await this.delete(message);
          this.metrics.sqsMessages.inc({ outcome: d.outcome });
          return true;
        case "retry": {
          const delay = Math.min(this.config.CONSUMER_RETRY_BASE_S * 2 ** (receiveCount - 1), 900);
          await this.sqs.send(
            new ChangeMessageVisibilityCommand({
              QueueUrl: this.queues.wager,
              ReceiptHandle: message.ReceiptHandle!,
              VisibilityTimeout: delay,
            }),
          );
          this.metrics.sqsMessages.inc({ outcome: "retried" });
          this.metrics.retry("sqs_consume", d.reason);
          return false;
        }
        case "dead_letter":
          await this.sqs.send(
            new SendMessageCommand({
              QueueUrl: this.queues.dlq,
              MessageBody: message.Body ?? "",
              MessageGroupId: message.Attributes?.MessageGroupId ?? "dead-letter",
              MessageDeduplicationId: message.MessageId!,
              MessageAttributes: {
                failureReason: { DataType: "String", StringValue: d.reason },
                errorCode: { DataType: "String", StringValue: d.code },
                receiveCount: { DataType: "Number", StringValue: String(receiveCount) },
              },
            }),
          );
          await this.delete(message);
          this.metrics.sqsMessages.inc({ outcome: "dead_lettered" });
          this.metrics.dlqMessages.inc({ reason: d.reason });
          return true;
      }
    } catch (err) {
      // Could not ack / move: the message simply becomes visible again and is redelivered.
      this.logger.warn("sqs disposition failed; message will be redelivered", { disposition: d.type, ...errorFields(err) });
      return false;
    }
  }

  private async delete(message: Message): Promise<void> {
    await this.sqs.send(new DeleteMessageCommand({ QueueUrl: this.queues.wager, ReceiptHandle: message.ReceiptHandle! }));
  }

  /** Makes not-yet-processed messages immediately visible to other instances. */
  private async release(messages: Message[]): Promise<void> {
    await Promise.allSettled(
      messages.map((m) =>
        this.sqs.send(
          new ChangeMessageVisibilityCommand({ QueueUrl: this.queues.wager, ReceiptHandle: m.ReceiptHandle!, VisibilityTimeout: 0 }),
        ),
      ),
    );
    if (messages.length) this.metrics.sqsMessages.inc({ outcome: "released" }, messages.length);
  }

  private faultInjection(point: string): void {
    if (this.config.FAULT_INJECTION === point) {
      this.logger.error("fault injection: crashing", { point });
      process.kill(process.pid, "SIGKILL");
    }
  }
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
