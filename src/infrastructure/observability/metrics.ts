import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from "prom-client";
import type { WageringMetrics } from "../../application/ports";

/** Prometheus registry. Gauges backed by the database are refreshed lazily on scrape. */
export class PromMetrics implements WageringMetrics {
  readonly registry = new Registry();

  private readonly transactions = new Counter({
    name: "wagering_transactions_total",
    help: "Settled wager transactions by kind, final status and entry point",
    labelNames: ["kind", "status", "source"],
    registers: [this.registry],
  });
  private readonly duplicates = new Counter({
    name: "wagering_duplicates_total",
    help: "Duplicates detected (idempotent replay, inbox redelivery, conflicting payload)",
    labelNames: ["type"],
    registers: [this.registry],
  });
  private readonly retries = new Counter({
    name: "wagering_retries_total",
    help: "Retries by operation and reason",
    labelNames: ["operation", "reason"],
    registers: [this.registry],
  });
  private readonly lockConflicts = new Counter({
    name: "wagering_lock_conflicts_total",
    help: "Wallet lock contention / concurrency conflicts",
    labelNames: ["type"],
    registers: [this.registry],
  });
  private readonly latency = new Histogram({
    name: "wagering_processing_duration_seconds",
    help: "End-to-end processing latency of a wager transaction",
    labelNames: ["source"],
    buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
    registers: [this.registry],
  });
  private readonly reconciliations = new Counter({
    name: "wagering_reconciliations_total",
    help: "Wallet reconciliations by result",
    labelNames: ["result"],
    registers: [this.registry],
  });
  readonly sqsMessages = new Counter({
    name: "sqs_consumer_messages_total",
    help: "Consumed messages by outcome (acked, duplicate, retried, dead_lettered, released)",
    labelNames: ["outcome"],
    registers: [this.registry],
  });
  readonly dlqMessages = new Counter({
    name: "sqs_dead_lettered_messages_total",
    help: "Messages sent to the DLQ by this instance, by reason",
    labelNames: ["reason"],
    registers: [this.registry],
  });
  readonly dlqDepth = new Gauge({
    name: "sqs_dlq_depth",
    help: "Approximate number of messages currently in the DLQ",
    registers: [this.registry],
  });
  readonly outboxPublished = new Counter({
    name: "outbox_published_total",
    help: "Outbox events published",
    labelNames: ["event_type"],
    registers: [this.registry],
  });
  readonly outboxFailures = new Counter({
    name: "outbox_publish_failures_total",
    help: "Outbox publish failures (rescheduled with backoff)",
    registers: [this.registry],
  });
  readonly outboxLag = new Gauge({
    name: "outbox_lag_seconds",
    help: "Age of the oldest unpublished outbox event",
    registers: [this.registry],
  });
  readonly outboxPending = new Gauge({
    name: "outbox_pending_messages",
    help: "Unpublished outbox events",
    registers: [this.registry],
  });
  readonly pendingReferences = new Gauge({
    name: "wagering_pending_reference_transactions",
    help: "Transactions waiting for their reference",
    registers: [this.registry],
  });

  constructor() {
    collectDefaultMetrics({ register: this.registry });
  }

  transactionFinished(kind: string, status: string, source: string) {
    this.transactions.inc({ kind, status, source });
  }
  duplicateDetected(type: string) {
    this.duplicates.inc({ type });
  }
  retry(operation: string, reason: string) {
    this.retries.inc({ operation, reason });
  }
  lockConflict(type: string) {
    this.lockConflicts.inc({ type });
  }
  processingLatency(source: string, seconds: number) {
    this.latency.observe({ source }, seconds);
  }
  reconciliation(consistent: boolean) {
    this.reconciliations.inc({ result: consistent ? "consistent" : "divergent" });
  }
}
