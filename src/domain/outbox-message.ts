import type { IntegrationEvent } from "./events/integration-event";

export interface OutboxMessageState {
  id: string;
  aggregateId: string;
  eventType: string;
  payload: Readonly<Record<string, unknown>>;
  occurredAt: Date;
  attempts: number;
  nextAttemptAt?: Date;
  publishedAt?: Date;
}

export const OUTBOX_BASE_DELAY_MS = 1_000;
export const OUTBOX_MAX_DELAY_MS = 5 * 60_000;

/** An integration event waiting to be published. Written in the same SQL transaction as the change. */
export class OutboxMessage {
  private constructor(
    public readonly id: string,
    public readonly aggregateId: string,
    public readonly eventType: string,
    public readonly payload: Readonly<Record<string, unknown>>,
    public readonly occurredAt: Date,
    private _attempts: number,
    private _nextAttemptAt?: Date,
    private _publishedAt?: Date,
  ) {}

  static enqueue(event: IntegrationEvent<unknown>): OutboxMessage {
    return new OutboxMessage(
      event.eventId,
      event.aggregateId,
      event.eventType,
      Object.freeze(event.toJSON() as unknown as Record<string, unknown>),
      event.occurredAt,
      0,
      event.occurredAt,
    );
  }

  static rehydrate(s: OutboxMessageState): OutboxMessage {
    return new OutboxMessage(s.id, s.aggregateId, s.eventType, s.payload, s.occurredAt, s.attempts, s.nextAttemptAt, s.publishedAt);
  }

  get attempts(): number {
    return this._attempts;
  }
  get nextAttemptAt(): Date | undefined {
    return this._nextAttemptAt;
  }
  get publishedAt(): Date | undefined {
    return this._publishedAt;
  }

  isPending(): boolean {
    return this._publishedAt === undefined;
  }

  isDue(now: Date): boolean {
    return this.isPending() && (!this._nextAttemptAt || this._nextAttemptAt.getTime() <= now.getTime());
  }

  markPublished(at: Date): void {
    if (this._publishedAt) return;
    this._publishedAt = at;
    this._nextAttemptAt = undefined;
  }

  /** Increments attempts and computes the next attempt with capped exponential backoff + jitter. */
  scheduleRetry(now: Date, random: () => number = Math.random): void {
    this._attempts += 1;
    const exp = Math.min(OUTBOX_BASE_DELAY_MS * 2 ** (this._attempts - 1), OUTBOX_MAX_DELAY_MS);
    const jitter = Math.floor(exp * 0.2 * random());
    this._nextAttemptAt = new Date(now.getTime() + exp + jitter);
  }
}
