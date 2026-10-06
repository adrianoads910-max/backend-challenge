import { hostname } from "node:os";
import { z } from "zod";

const bool = z
  .enum(["true", "false", "1", "0"])
  .transform((v) => v === "true" || v === "1");
const int = (def: number) => z.coerce.number().int().nonnegative().default(def);

const schema = z.object({
  PORT: int(3000),
  INSTANCE_ID: z.string().default(`${hostname()}-${process.pid}`),
  LOG_LEVEL: z.enum(["trace", "debug", "info", "warn", "error", "fatal", "silent"]).default("info"),

  DATABASE_URL: z.string().default("postgres://wagering:wagering@localhost:5442/wagering"),
  DB_POOL_MAX: int(10),
  DB_STATEMENT_TIMEOUT_MS: int(10_000),
  DB_LOCK_TIMEOUT_MS: int(5_000),

  AWS_REGION: z.string().default("us-east-1"),
  AWS_ACCESS_KEY_ID: z.string().default("test"),
  AWS_SECRET_ACCESS_KEY: z.string().default("test"),
  SQS_ENDPOINT: z.string().default("http://localhost:4566"),
  SQS_WAGER_QUEUE: z.string().default("wager-transactions.fifo"),
  SQS_WAGER_DLQ: z.string().default("wager-transactions-dlq.fifo"),
  SQS_EVENTS_QUEUE: z.string().default("wagering-events.fifo"),
  SQS_AUTO_CREATE_QUEUES: bool.default(true),

  ENABLE_HTTP: bool.default(true),
  ENABLE_CONSUMER: bool.default(true),
  ENABLE_OUTBOX_PUBLISHER: bool.default(true),
  ENABLE_PENDING_REFERENCE_WORKER: bool.default(true),

  CONSUMER_MAX_RECEIVES: int(5),
  CONSUMER_VISIBILITY_TIMEOUT_S: int(30),
  CONSUMER_WAIT_TIME_S: int(5),
  CONSUMER_RETRY_BASE_S: int(2),
  /** Redrive policy on the queue itself — a backstop above CONSUMER_MAX_RECEIVES. */
  SQS_REDRIVE_MAX_RECEIVES: int(10),

  OUTBOX_POLL_MS: int(500),
  OUTBOX_BATCH_SIZE: int(50),
  OUTBOX_LEASE_MS: int(30_000),

  PENDING_REFERENCE_POLL_MS: int(1_000),
  PENDING_REFERENCE_MAX_ATTEMPTS: int(8),
  PENDING_REFERENCE_BASE_DELAY_MS: int(2_000),
  PENDING_REFERENCE_MAX_DELAY_MS: int(300_000),

  /** Test-only fault injection: crash_after_commit_before_ack | crash_after_publish_before_mark */
  FAULT_INJECTION: z.string().optional(),
});

export type AppConfig = z.infer<typeof schema>;

export function loadConfig(env: Record<string, string | undefined> = process.env): AppConfig {
  return schema.parse(env);
}

export const APP_CONFIG = Symbol("AppConfig");
