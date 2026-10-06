import {
  CreateQueueCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  PurgeQueueCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import type { INestApplication } from "@nestjs/common";
import { SQL } from "bun";
import { expect } from "bun:test";
import { createServer } from "node:net";
import { migrate } from "../../src/migrate";

/*
 * Real infrastructure for integration tests: PostgreSQL and MiniStack (SQS) from docker compose.
 * Every test file gets its own database and its own queues, so files are fully isolated and can
 * assert on global state (all wallets, all outbox rows, whole queues).
 */

const ADMIN_URL = process.env.TEST_DATABASE_ADMIN_URL ?? "postgres://wagering:wagering@localhost:5442/wagering";
const SQS_ENDPOINT = process.env.TEST_SQS_ENDPOINT ?? "http://localhost:4566";

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export async function eventually<T>(
  fn: () => Promise<T>,
  { timeoutMs = 20_000, intervalMs = 100, message = "condition" } = {},
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      return await fn();
    } catch (err) {
      lastError = err;
      await sleep(intervalMs);
    }
  }
  throw new Error(`timed out waiting for ${message}: ${String((lastError as Error)?.message ?? lastError)}`);
}

export async function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = createServer();
    srv.listen(0, () => {
      const port = (srv.address() as { port: number }).port;
      srv.close(() => resolve(port));
    });
    srv.on("error", reject);
  });
}

export interface Instance {
  baseUrl: string;
  stop(): Promise<void>;
}

export interface SpawnedInstance extends Instance {
  proc: ReturnType<typeof Bun.spawn>;
  kill(signal?: NodeJS.Signals): void;
  exited: Promise<number>;
  logs(): string;
}

export class TestEnv {
  readonly sql: SQL;
  readonly sqs: SQSClient;
  private readonly instances: Instance[] = [];
  private queueUrlCache = new Map<string, string>();

  private constructor(
    readonly name: string,
    readonly databaseUrl: string,
    readonly queues: { wager: string; dlq: string; events: string },
  ) {
    this.sql = new SQL(databaseUrl, { max: 10 });
    this.sqs = new SQSClient({
      endpoint: SQS_ENDPOINT,
      region: "us-east-1",
      credentials: { accessKeyId: "test", secretAccessKey: "test" },
    });
  }

  /** Fresh database (migrated) + dedicated queues. `standardEvents` makes duplicates observable. */
  static async create(label: string, opts: { standardEvents?: boolean } = {}): Promise<TestEnv> {
    const suffix = `${label}_${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`.toLowerCase();
    const dbName = `wagering_test_${suffix}`.replace(/[^a-z0-9_]/g, "_");
    const admin = new SQL(ADMIN_URL, { max: 1 });
    await admin.unsafe(`create database ${dbName}`);
    await admin.close();
    const databaseUrl = ADMIN_URL.replace(/\/[^/]+$/, `/${dbName}`);
    await migrate("up", databaseUrl);
    const q = suffix.replace(/_/g, "-");
    return new TestEnv(label, databaseUrl, {
      wager: `wt-${q}.fifo`,
      dlq: `wt-dlq-${q}.fifo`,
      events: opts.standardEvents ? `ev-${q}` : `ev-${q}.fifo`,
    });
  }

  /** Environment for an app instance bound to this test's database and queues. */
  appEnv(overrides: Record<string, string> = {}): Record<string, string> {
    return {
      PATH: process.env.PATH ?? "",
      HOME: process.env.HOME ?? "",
      DATABASE_URL: this.databaseUrl,
      SQS_ENDPOINT,
      SQS_WAGER_QUEUE: this.queues.wager,
      SQS_WAGER_DLQ: this.queues.dlq,
      SQS_EVENTS_QUEUE: this.queues.events,
      LOG_LEVEL: process.env.TEST_LOG_LEVEL ?? "silent",
      DB_POOL_MAX: "10",
      CONSUMER_WAIT_TIME_S: "1",
      CONSUMER_VISIBILITY_TIMEOUT_S: "5",
      CONSUMER_RETRY_BASE_S: "1",
      CONSUMER_MAX_RECEIVES: "5",
      OUTBOX_POLL_MS: "100",
      OUTBOX_LEASE_MS: "3000",
      PENDING_REFERENCE_POLL_MS: "100",
      PENDING_REFERENCE_BASE_DELAY_MS: "200",
      PENDING_REFERENCE_MAX_DELAY_MS: "1000",
      PENDING_REFERENCE_MAX_ATTEMPTS: "8",
      ...overrides,
    };
  }

  /** NestJS app inside the test process (fast; shares nothing with others but the DB/queues). */
  async startInProcess(overrides: Record<string, string> = {}): Promise<Instance & { app: INestApplication }> {
    const { bootstrap } = await import("../../src/main");
    const app = await bootstrap(this.appEnv({ PORT: "0", ...overrides }));
    const address = app.getHttpServer().address() as { port: number } | null;
    const instance = {
      app,
      baseUrl: `http://127.0.0.1:${address?.port ?? 0}`,
      stop: async () => {
        await app.close();
      },
    };
    this.instances.push(instance);
    return instance;
  }

  /** A real, separate OS process running `bun src/main.ts`. */
  async spawn(overrides: Record<string, string> = {}, { waitReady = true } = {}): Promise<SpawnedInstance> {
    const port = await freePort();
    let output = "";
    const proc = Bun.spawn(["bun", "src/main.ts"], {
      cwd: `${import.meta.dir}/../..`,
      env: this.appEnv({ PORT: String(port), INSTANCE_ID: `test-${port}`, LOG_LEVEL: "info", ...overrides }),
      stdout: "pipe",
      stderr: "pipe",
    });
    const drain = async (stream: ReadableStream<Uint8Array>) => {
      const decoder = new TextDecoder();
      for await (const chunk of stream) output += decoder.decode(chunk);
    };
    void drain(proc.stdout as ReadableStream<Uint8Array>);
    void drain(proc.stderr as ReadableStream<Uint8Array>);
    const baseUrl = `http://127.0.0.1:${port}`;
    const instance: SpawnedInstance = {
      proc,
      baseUrl,
      exited: proc.exited,
      logs: () => output,
      kill: (signal = "SIGKILL") => proc.kill(signal),
      stop: async () => {
        if (proc.exitCode === null && !proc.killed) proc.kill("SIGTERM");
        await Promise.race([proc.exited, sleep(15_000)]);
        if (proc.exitCode === null) proc.kill("SIGKILL");
      },
    };
    this.instances.push(instance);
    if (waitReady) {
      await eventually(
        async () => {
          if (proc.exitCode !== null) throw new Error(`instance exited early:\n${output}`);
          const res = await fetch(`${baseUrl}/health/ready`);
          if (res.status !== 200) throw new Error(`ready=${res.status}`);
        },
        { timeoutMs: 30_000, message: `instance on ${port} to be ready` },
      );
    }
    return instance;
  }

  // ------------------------------------------------------------------ HTTP helpers

  async createWallet(baseUrl: string, amount = "100.00", currency = "BRL", playerId: string = crypto.randomUUID()) {
    const res = await fetch(`${baseUrl}/wallets`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ playerId, initialBalance: { amount, currency } }),
    });
    expect(res.status).toBe(201);
    const body = (await res.json()) as { id: string; playerId: string };
    return { walletId: body.id, playerId: body.playerId };
  }

  async submit(baseUrl: string, payload: WagerPayload, idempotencyKey = `${payload.providerId}:${payload.externalTransactionId}`) {
    const res = await fetch(`${baseUrl}/wagering/transactions`, {
      method: "POST",
      headers: { "content-type": "application/json", "idempotency-key": idempotencyKey },
      body: JSON.stringify(payload),
    });
    return { status: res.status, body: (await res.json()) as SubmitResponse };
  }

  // ------------------------------------------------------------------ SQS helpers

  async queueUrl(name: string): Promise<string> {
    const cached = this.queueUrlCache.get(name);
    if (cached) return cached;
    const attributes: Record<string, string> = name.endsWith(".fifo") ? { FifoQueue: "true" } : {};
    const url = (await this.sqs.send(new CreateQueueCommand({ QueueName: name, Attributes: attributes }))).QueueUrl!;
    this.queueUrlCache.set(name, url);
    return url;
  }

  async sendWagerMessage(payload: WagerPayload, messageId: string = crypto.randomUUID(), opts: { raw?: string; dedupId?: string } = {}) {
    const body =
      opts.raw ??
      JSON.stringify({
        messageId,
        type: "WagerTransactionRequested",
        occurredAt: new Date().toISOString(),
        data: { ...payload, idempotencyKey: `${payload.providerId}:${payload.externalTransactionId}` },
      });
    await this.sqs.send(
      new SendMessageCommand({
        QueueUrl: await this.queueUrl(this.queues.wager),
        MessageBody: body,
        MessageGroupId: payload.walletId,
        MessageDeduplicationId: opts.dedupId ?? crypto.randomUUID(),
      }),
    );
    return messageId;
  }

  /** Drains a queue (deleting nothing: messages become visible again after `visibility`). */
  async receiveAll(queue: string, { waitMs = 2_000, visibility = 60 } = {}) {
    const url = await this.queueUrl(queue);
    const out: { body: string; attributes: Record<string, string> }[] = [];
    const deadline = Date.now() + waitMs;
    while (Date.now() < deadline) {
      const res = await this.sqs.send(
        new ReceiveMessageCommand({
          QueueUrl: url,
          MaxNumberOfMessages: 10,
          WaitTimeSeconds: 1,
          VisibilityTimeout: visibility,
          MessageAttributeNames: ["All"],
        }),
      );
      for (const m of res.Messages ?? []) {
        const attributes: Record<string, string> = {};
        for (const [k, v] of Object.entries(m.MessageAttributes ?? {})) attributes[k] = v.StringValue ?? "";
        out.push({ body: m.Body ?? "", attributes });
      }
    }
    return out;
  }

  async queueDepth(queue: string): Promise<{ visible: number; inFlight: number }> {
    const res = await this.sqs.send(
      new GetQueueAttributesCommand({
        QueueUrl: await this.queueUrl(queue),
        AttributeNames: ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
      }),
    );
    return {
      visible: Number(res.Attributes?.ApproximateNumberOfMessages ?? 0),
      inFlight: Number(res.Attributes?.ApproximateNumberOfMessagesNotVisible ?? 0),
    };
  }

  async purge(queue: string) {
    await this.sqs.send(new PurgeQueueCommand({ QueueUrl: await this.queueUrl(queue) })).catch(() => undefined);
  }

  // ------------------------------------------------------------------ DB assertions

  async walletRow(walletId: string) {
    const [row] = await this.sql`select balance::text as balance, version from wallets where id = ${walletId}`;
    return row as { balance: string; version: number };
  }

  async ledgerRows(walletId: string) {
    return (await this.sql`
      select direction, amount::text as amount, balance_before::text as before, balance_after::text as after,
             wallet_version as version, transaction_id
        from wallet_ledger_entries where wallet_id = ${walletId} order by wallet_version`) as {
      direction: string;
      amount: string;
      before: string;
      after: string;
      version: number;
      transaction_id: string;
    }[];
  }

  async transactionsOf(walletId: string) {
    return (await this.sql`
      select id, kind, status, failure_code, external_transaction_id, amount::text as amount
        from wager_transactions where wallet_id = ${walletId} and kind <> 'OPENING' order by created_at`) as {
      id: string;
      kind: string;
      status: string;
      failure_code: string | null;
      external_transaction_id: string;
      amount: string;
    }[];
  }

  /**
   * The final invariant of every test: for EVERY wallet, the stored balance equals the balance
   * rebuilt from the ledger, the version equals the latest ledger version, and the ledger chains.
   */
  async assertGlobalInvariants(): Promise<void> {
    const diverging = await this.sql`
      select w.id, w.balance::text as balance, w.version,
             coalesce(sum(case when l.direction = 'CREDIT' then l.amount else -l.amount end), 0)::text as rebuilt,
             coalesce(max(l.wallet_version), 1) as last_version
        from wallets w left join wallet_ledger_entries l on l.wallet_id = w.id
       group by w.id
      having w.balance <> coalesce(sum(case when l.direction = 'CREDIT' then l.amount else -l.amount end), 0)
          or w.version <> coalesce(max(l.wallet_version), 1)`;
    expect(diverging).toEqual([]);
    const broken = await this.sql`
      select l.id from wallet_ledger_entries l
        left join wallet_ledger_entries p on p.wallet_id = l.wallet_id and p.wallet_version = l.wallet_version - 1
       where (p.id is null and l.balance_before <> 0) or (p.id is not null and p.balance_after <> l.balance_before)`;
    expect(broken).toEqual([]);
    const negative = await this.sql`select id from wallets where balance < 0`;
    expect(negative).toEqual([]);
    // One ledger entry per balance-moving PROCESSED transaction, none for anything else.
    const mismatched = await this.sql`
      select t.id from wager_transactions t
        left join wallet_ledger_entries l on l.transaction_id = t.id
       where (t.status = 'PROCESSED' and t.kind <> 'LOSS') <> (l.id is not null)`;
    expect(mismatched).toEqual([]);
  }

  async dispose(): Promise<void> {
    await Promise.allSettled(this.instances.map((i) => i.stop()));
    await this.sql.close();
    for (const q of [this.queues.wager, this.queues.dlq, this.queues.events]) {
      await this.sqs.send(new DeleteQueueCommand({ QueueUrl: await this.queueUrl(q) })).catch(() => undefined);
    }
    const admin = new SQL(ADMIN_URL, { max: 1 });
    const dbName = this.databaseUrl.split("/").pop()!;
    await admin.unsafe(`drop database if exists ${dbName} with (force)`).catch(() => undefined);
    await admin.close();
  }
}

export interface WagerPayload {
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: "BET" | "WIN" | "LOSS" | "REFUND" | "ROLLBACK";
  money: { amount: string; currency: string };
  referenceExternalTransactionId?: string;
}

export interface SubmitResponse {
  transactionId: string;
  status: string;
  failureCode?: string;
  balance?: { amount: string; currency: string };
  idempotentReplay: boolean;
  error?: { code: string; message: string };
}

export function wager(
  wallet: { walletId: string; playerId: string },
  kind: WagerPayload["kind"],
  amount: string,
  extra: Partial<WagerPayload> = {},
): WagerPayload {
  return {
    providerId: "provider-a",
    externalTransactionId: `tx-${crypto.randomUUID()}`,
    playerId: wallet.playerId,
    walletId: wallet.walletId,
    roundId: "round-1",
    gameId: "fortune-chimp",
    kind,
    money: { amount, currency: "BRL" },
    ...extra,
  };
}
