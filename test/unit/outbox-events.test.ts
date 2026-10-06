import { describe, expect, test } from "bun:test";
import { WalletBalanceChanged } from "../../src/domain/events/wagering-events";
import { InboxMessage } from "../../src/domain/inbox-message";
import { Money } from "../../src/domain/money";
import { OutboxMessage } from "../../src/domain/outbox-message";
import { Wallet } from "../../src/domain/wallet";

const now = new Date("2026-01-01T00:00:00Z");
const brl = (a: string) => Money.from({ amount: a, currency: "BRL" });

function balanceChanged() {
  const { wallet } = Wallet.open({ id: "w", playerId: "p", initialBalance: brl("10.00"), openingTransactionId: "o", openingEntryId: "e0", at: now });
  const entry = wallet.debit({ entryId: "e1", transactionId: "t1", money: brl("2.50"), at: now });
  return WalletBalanceChanged.from(wallet, entry, { eventId: "ev-1", correlationId: "c-1", occurredAt: now });
}

describe("integration events", () => {
  test("envelope carries type and version from the class and MoneyProps strings in data", () => {
    const json = balanceChanged().toJSON();
    expect(json).toEqual({
      eventId: "ev-1",
      eventType: "WalletBalanceChanged",
      aggregateId: "w",
      correlationId: "c-1",
      occurredAt: "2026-01-01T00:00:00.000Z",
      version: 1,
      data: {
        walletId: "w",
        transactionId: "t1",
        direction: "DEBIT",
        money: { amount: "2.50", currency: "BRL" },
        balanceBefore: { amount: "10.00", currency: "BRL" },
        balanceAfter: { amount: "7.50", currency: "BRL" },
        walletVersion: 2,
      },
    });
    // stable JSON: no class instances inside
    expect(JSON.parse(JSON.stringify(json))).toEqual(json);
  });
});

describe("OutboxMessage", () => {
  test("enqueue is pending and due immediately", () => {
    const m = OutboxMessage.enqueue(balanceChanged());
    expect(m.isPending()).toBe(true);
    expect(m.isDue(now)).toBe(true);
    expect(m.eventType).toBe("WalletBalanceChanged");
    expect(m.id).toBe("ev-1");
  });

  test("scheduleRetry uses capped exponential backoff", () => {
    const m = OutboxMessage.enqueue(balanceChanged());
    const delays: number[] = [];
    for (let i = 0; i < 12; i++) {
      m.scheduleRetry(now, () => 0);
      delays.push(m.nextAttemptAt!.getTime() - now.getTime());
    }
    expect(m.attempts).toBe(12);
    expect(delays.slice(0, 4)).toEqual([1_000, 2_000, 4_000, 8_000]);
    expect(Math.max(...delays)).toBe(300_000);
    expect(m.isDue(now)).toBe(false);
  });

  test("markPublished is final", () => {
    const m = OutboxMessage.enqueue(balanceChanged());
    m.markPublished(now);
    expect(m.isPending()).toBe(false);
    expect(m.isDue(now)).toBe(false);
  });
});

describe("InboxMessage", () => {
  test("markProcessed is idempotent", () => {
    const m = InboxMessage.receive({ messageId: "m", consumerName: "c", payloadHash: "h", receivedAt: now });
    expect(m.isProcessed()).toBe(false);
    m.markProcessed(now);
    m.markProcessed(new Date(0));
    expect(m.processedAt).toEqual(now);
  });
});
