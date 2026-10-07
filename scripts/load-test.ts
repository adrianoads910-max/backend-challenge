/**
 * Load test against running instances (default: the three docker compose apps).
 *
 *   docker compose up -d --build
 *   bun run test:load                      # 60s, 64 concurrent clients
 *   LOAD_DURATION_S=30 LOAD_CONCURRENCY=32 LOAD_TARGETS=http://localhost:3101 bun run test:load
 *
 * Workload: many "normal" wallets + a few hot wallets (20% of traffic), mixed BET/WIN/LOSS, and
 * 5% deliberate replays of already-sent operations. Measures latency percentiles, status mix,
 * transient errors, lock conflicts and outbox lag, then proves consistency with reconciliation.
 */

import {
  CreateQueueCommand,
  DeleteMessageBatchCommand,
  PurgeQueueCommand,
  ReceiveMessageCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";

const targets = (process.env.LOAD_TARGETS ?? "http://localhost:3101,http://localhost:3102,http://localhost:3103").split(",");
const durationS = Number(process.env.LOAD_DURATION_S ?? 60);
const concurrency = Number(process.env.LOAD_CONCURRENCY ?? 64);
const normalWallets = Number(process.env.LOAD_WALLETS ?? 200);
const hotWallets = Number(process.env.LOAD_HOT_WALLETS ?? 2);
const hotShare = Number(process.env.LOAD_HOT_SHARE ?? 0.2);
const eventsQueue = process.env.LOAD_EVENTS_QUEUE ?? "wagering-events.fifo";
const sqs = new SQSClient({
  endpoint: process.env.LOAD_SQS_ENDPOINT ?? "http://localhost:4566",
  region: "us-east-1",
  credentials: { accessKeyId: "test", secretAccessKey: "test" },
});

/**
 * Downstream consumer of the integration events: receives + deletes, counting unique eventIds and
 * duplicates. Without it the emulated queue only grows, and MiniStack's send latency grows with
 * queue depth (~9k msg/s on an empty FIFO queue, ~140 msg/s at 80k messages) — that would measure
 * the emulator, not the outbox.
 */
const eventIds = new Set<string>();
let eventDuplicates = 0;
let consuming = true;
async function consumeEvents(queueUrl: string) {
  while (consuming) {
    try {
      const res = await sqs.send(
        new ReceiveMessageCommand({ QueueUrl: queueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 1, VisibilityTimeout: 30 }),
      );
      const messages = res.Messages ?? [];
      for (const m of messages) {
        const id = (JSON.parse(m.Body ?? "{}") as { eventId?: string }).eventId ?? "";
        if (eventIds.has(id)) eventDuplicates++;
        else eventIds.add(id);
      }
      if (messages.length) {
        await sqs.send(
          new DeleteMessageBatchCommand({
            QueueUrl: queueUrl,
            Entries: messages.map((m, i) => ({ Id: String(i), ReceiptHandle: m.ReceiptHandle! })),
          }),
        );
      }
    } catch {
      await Bun.sleep(200);
    }
  }
}

type Wallet = { walletId: string; playerId: string };
type Op = { payload: Record<string, unknown>; key: string };

const latencies: number[] = [];
const statuses = new Map<string, number>();
const sent: Op[] = [];
let replays = 0;

const count = (k: string) => statuses.set(k, (statuses.get(k) ?? 0) + 1);
const target = () => targets[Math.floor(Math.random() * targets.length)]!;

async function createWallet(amount: string): Promise<Wallet> {
  const playerId = crypto.randomUUID();
  const res = await fetch(`${target()}/wallets`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ playerId, initialBalance: { amount, currency: "BRL" } }),
  });
  if (res.status !== 201) throw new Error(`wallet creation failed: ${res.status} ${await res.text()}`);
  return { walletId: ((await res.json()) as { id: string }).id, playerId };
}

function newOp(w: Wallet): Op {
  const r = Math.random();
  const kind = r < 0.7 ? "BET" : r < 0.95 ? "WIN" : "LOSS";
  const amount = kind === "BET" ? "1.00" : kind === "WIN" ? "1.50" : "0.00";
  const externalTransactionId = `load-${crypto.randomUUID()}`;
  return {
    key: `load:${externalTransactionId}`,
    payload: {
      providerId: "load", externalTransactionId, playerId: w.playerId, walletId: w.walletId,
      roundId: `r-${Math.floor(Math.random() * 1000)}`, gameId: "fortune-chimp", kind,
      money: { amount, currency: "BRL" },
    },
  };
}

async function submit(op: Op) {
  const t0 = performance.now();
  try {
    const res = await fetch(`${target()}/wagering/transactions`, {
      method: "POST",
      headers: { "content-type": "application/json", "idempotency-key": op.key },
      body: JSON.stringify(op.payload),
    });
    await res.arrayBuffer();
    latencies.push(performance.now() - t0);
    count(String(res.status));
  } catch {
    count("network_error");
  }
}

async function scrape(): Promise<Map<string, number>> {
  const totals = new Map<string, number>();
  for (const t of targets) {
    const text = await (await fetch(`${t}/metrics`)).text();
    for (const line of text.split("\n")) {
      if (line.startsWith("#") || !line.trim()) continue;
      const i = line.lastIndexOf(" ");
      totals.set(line.slice(0, i), (totals.get(line.slice(0, i)) ?? 0) + Number(line.slice(i + 1)));
    }
  }
  return totals;
}

const sum = (m: Map<string, number>, prefix: string) =>
  [...m.entries()].filter(([k]) => k.startsWith(prefix)).reduce((a, [, v]) => a + v, 0);

function percentile(sorted: number[], p: number) {
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))] ?? 0;
}

async function main() {
  console.log(`targets=${targets.join(",")} duration=${durationS}s concurrency=${concurrency}`);
  const normal = await Promise.all(Array.from({ length: normalWallets }, () => createWallet("1000000.00")));
  const hot = await Promise.all(Array.from({ length: hotWallets }, () => createWallet("1000000.00")));
  const eventsUrl = (await sqs.send(new CreateQueueCommand({ QueueName: eventsQueue, Attributes: eventsQueue.endsWith(".fifo") ? { FifoQueue: "true" } : {} }))).QueueUrl!;
  await sqs.send(new PurgeQueueCommand({ QueueUrl: eventsUrl })).catch(() => undefined);
  const consumers = Array.from({ length: 8 }, () => consumeEvents(eventsUrl));
  const before = await scrape();
  // A backlog left by a previous run would distort the lag numbers: report it.
  const initialBacklog = sum(before, "outbox_pending_messages") / targets.length;

  let maxLag = 0;
  const lagSamples: number[] = [];
  const deadline = Date.now() + durationS * 1000;
  const sampler = (async () => {
    while (Date.now() < deadline) {
      try {
        const text = await (await fetch(`${targets[0]}/metrics`)).text();
        const lag = Number(/^outbox_lag_seconds (\S+)/m.exec(text)?.[1] ?? 0);
        lagSamples.push(lag);
        maxLag = Math.max(maxLag, lag);
      } catch {
        /* ignore */
      }
      await Bun.sleep(1_000);
    }
  })();

  const started = performance.now();
  await Promise.all(
    Array.from({ length: concurrency }, async () => {
      while (Date.now() < deadline) {
        if (sent.length > 0 && Math.random() < 0.05) {
          replays++;
          await submit(sent[Math.floor(Math.random() * sent.length)]!);
          continue;
        }
        const pool = hot.length > 0 && Math.random() < hotShare ? hot : normal;
        const op = newOp(pool[Math.floor(Math.random() * pool.length)]!);
        sent.push(op);
        await submit(op);
      }
    }),
  );
  const elapsedS = (performance.now() - started) / 1000;
  await sampler;

  // let the outbox drain, then reconcile every wallet
  const drainStart = performance.now();
  let drained = false;
  for (let i = 0; i < 600 && !drained; i++) {
    const text = await (await fetch(`${targets[0]}/metrics`)).text();
    drained = Number(/^outbox_pending_messages (\S+)/m.exec(text)?.[1] ?? 1) === 0;
    if (!drained) await Bun.sleep(500);
  }
  const drainS = (performance.now() - drainStart) / 1000;
  const after = await scrape();
  await Bun.sleep(3_000); // let the downstream consumer catch the tail
  consuming = false;
  await Promise.all(consumers);

  let inconsistent = 0;
  for (const w of [...normal, ...hot]) {
    const r = (await (await fetch(`${target()}/wallets/${w.walletId}/reconciliation`, { method: "POST" })).json()) as { consistent: boolean };
    if (!r.consistent) inconsistent++;
  }

  const sorted = [...latencies].sort((a, b) => a - b);
  const total = latencies.length + (statuses.get("network_error") ?? 0);
  const errors = total - (statuses.get("200") ?? 0) - (statuses.get("422") ?? 0) - (statuses.get("202") ?? 0);
  const delta = (prefix: string) => sum(after, prefix) - sum(before, prefix);
  const report = {
    environment: { targets: targets.length, concurrency, durationS, wallets: normalWallets, hotWallets, hotShare, runtime: `bun ${Bun.version}` },
    requests: total,
    throughputRps: Math.round(total / elapsedS),
    latencyMs: {
      p50: +percentile(sorted, 50).toFixed(1),
      p95: +percentile(sorted, 95).toFixed(1),
      p99: +percentile(sorted, 99).toFixed(1),
      max: +(sorted.at(-1) ?? 0).toFixed(1),
    },
    statuses: Object.fromEntries([...statuses.entries()].sort()),
    errorRate: +(errors / total).toFixed(4),
    deliberateReplays: replays,
    lockConflicts: {
      contended: delta('wagering_lock_conflicts_total{type="contended"}'),
      other: delta("wagering_lock_conflicts_total") - delta('wagering_lock_conflicts_total{type="contended"}'),
    },
    duplicatesDetected: delta("wagering_duplicates_total"),
    outboxLagSeconds: {
      max: +maxLag.toFixed(2),
      avg: +(lagSamples.reduce((a, b) => a + b, 0) / Math.max(1, lagSamples.length)).toFixed(2),
      drainAfterLoadS: +drainS.toFixed(1),
      drained,
      initialBacklog,
    },
    eventsDelivered: { unique: eventIds.size, duplicates: eventDuplicates },
    reconciliation: { wallets: normal.length + hot.length, inconsistent },
  };
  console.log(JSON.stringify(report, null, 2));
  if (inconsistent > 0) process.exit(1);
}

await main();
export {};
