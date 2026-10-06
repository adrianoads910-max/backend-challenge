import { describe, expect, test } from "bun:test";
import { InvalidTransactionError, InvalidTransactionStateError } from "../../src/domain/errors";
import { FailureCode } from "../../src/domain/failure-code";
import { LedgerDirection } from "../../src/domain/ledger-entry";
import { Money } from "../../src/domain/money";
import { Wallet } from "../../src/domain/wallet";
import { settleWagerTransaction } from "../../src/domain/wager-settlement";
import {
  WagerTransaction,
  WagerTransactionKind as K,
  WagerTransactionStatus as S,
} from "../../src/domain/wager-transaction";

const brl = (amount: string) => Money.from({ amount, currency: "BRL" });
const now = new Date("2026-01-01T00:00:00Z");
const policy = { maxAttempts: 3, baseDelayMs: 1_000, maxDelayMs: 10_000 };
let seq = 0;
const id = () => `id-${++seq}`;

function wallet(amount = "100.00") {
  return Wallet.rehydrate({ id: "w1", playerId: "p1", currency: "BRL", balance: brl(amount), version: 1, createdAt: now, updatedAt: now });
}

function tx(kind: K, amount: string, extra: Partial<Parameters<typeof WagerTransaction.create>[0]> = {}) {
  return WagerTransaction.create({
    id: id(), providerId: "prov", externalTransactionId: `ext-${seq}`, idempotencyKey: `k-${seq}`, payloadHash: "h",
    walletId: "w1", playerId: "p1", roundId: "r1", gameId: "g", kind, money: brl(amount), createdAt: now, ...extra,
  });
}

/** A transaction already PROCESSED against the wallet (e.g. the referenced BET). */
function processed(kind: K, amount: string, w: Wallet, extra = {}) {
  const t = tx(kind, amount, extra);
  const outcome = settle(t, w);
  expect(outcome.type).toBe("processed");
  return t;
}

function settle(t: WagerTransaction, w: Wallet, ctx: { reference?: WagerTransaction; reversed?: boolean } = {}) {
  return settleWagerTransaction(t, w, {
    reference: ctx.reference, referenceAlreadyReversed: ctx.reversed ?? false, now, newEntryId: id, retryPolicy: policy,
  });
}

describe("WagerTransaction factory", () => {
  test("is born PENDING", () => {
    expect(tx(K.Bet, "1.00").status).toBe(S.Pending);
  });
  test("OPENING cannot be submitted", () => {
    expect(() => tx(K.Opening, "1.00")).toThrow(InvalidTransactionError);
  });
  test("REFUND and ROLLBACK require a reference", () => {
    expect(() => tx(K.Refund, "1.00")).toThrow(InvalidTransactionError);
    expect(() => tx(K.Rollback, "1.00")).toThrow(InvalidTransactionError);
    expect(tx(K.Refund, "1.00", { referenceExternalTransactionId: "b" }).requiresReference()).toBe(true);
  });
  test("balance-moving kinds need a positive amount; LOSS may be zero", () => {
    expect(() => tx(K.Bet, "0.00")).toThrow(InvalidTransactionError);
    expect(() => tx(K.Win, "0.00")).toThrow(InvalidTransactionError);
    expect(tx(K.Loss, "0.00").affectsBalance()).toBe(false);
  });
  test("cannot reference itself", () => {
    expect(() => tx(K.Win, "1.00", { externalTransactionId: "same", referenceExternalTransactionId: "same" })).toThrow(InvalidTransactionError);
  });
});

describe("WagerTransaction transitions", () => {
  test("terminal states cannot transition (programming error)", () => {
    const t = tx(K.Bet, "1.00");
    t.markProcessed(undefined, now, brl("1.00"));
    expect(t.isTerminal()).toBe(true);
    expect(() => t.reject(FailureCode.InsufficientFunds, now, brl("1.00"))).toThrow(InvalidTransactionStateError);
    expect(() => t.fail(FailureCode.ProcessingFailed, now)).toThrow(InvalidTransactionStateError);
    expect(() => t.markPendingReference(now, policy, brl("1.00"))).toThrow(InvalidTransactionStateError);

    const r = tx(K.Bet, "1.00");
    r.reject(FailureCode.InsufficientFunds, now, brl("0.00"));
    expect(() => r.markProcessed(undefined, now, brl("0.00"))).toThrow(InvalidTransactionStateError);
  });

  test("PENDING_REFERENCE backs off exponentially, then reports exhaustion", () => {
    const t = tx(K.Refund, "1.00", { referenceExternalTransactionId: "missing" });
    const delays: number[] = [];
    for (let i = 0; i <= policy.maxAttempts; i++) {
      expect(t.markPendingReference(now, policy, brl("1.00"))).toBe(true);
      delays.push(t.nextAttemptAt!.getTime() - now.getTime());
    }
    expect(delays).toEqual([1_000, 2_000, 4_000, 8_000]);
    expect(t.markPendingReference(now, policy, brl("1.00"))).toBe(false);
    t.reject(FailureCode.ReferenceNotFound, now, brl("1.00"));
    expect(t.status).toBe(S.Rejected);
  });

  test("matchesPayload compares the stored hash", () => {
    const t = tx(K.Bet, "1.00", { payloadHash: "abc" });
    expect(t.matchesPayload("abc")).toBe(true);
    expect(t.matchesPayload("abd")).toBe(false);
  });

  test("ledgerDirectionFor", () => {
    const w = wallet();
    const bet = processed(K.Bet, "10.00", w);
    const win = processed(K.Win, "10.00", w);
    expect(bet.ledgerDirectionFor()).toBe(LedgerDirection.Debit);
    expect(win.ledgerDirectionFor()).toBe(LedgerDirection.Credit);
    const rb = tx(K.Rollback, "10.00", { referenceExternalTransactionId: "x" });
    expect(rb.ledgerDirectionFor(bet)).toBe(LedgerDirection.Credit);
    expect(rb.ledgerDirectionFor(win)).toBe(LedgerDirection.Debit);
    expect(() => tx(K.Loss, "1.00").ledgerDirectionFor()).toThrow(InvalidTransactionError);
  });
});

describe("business rules", () => {
  test("BET debits and records the observed balance", () => {
    const w = wallet("100.00");
    const t = tx(K.Bet, "25.00");
    const out = settle(t, w);
    expect(out.type).toBe("processed");
    expect(out.type === "processed" && out.entry?.direction).toBe(LedgerDirection.Debit);
    expect(w.balance.amount).toBe("75.00");
    expect(t.status).toBe(S.Processed);
    expect(t.resultBalance?.amount).toBe("75.00");
  });

  test("BET without funds is REJECTED INSUFFICIENT_FUNDS and moves nothing", () => {
    const w = wallet("10.00");
    const t = tx(K.Bet, "10.01");
    expect(settle(t, w)).toEqual({ type: "rejected", code: FailureCode.InsufficientFunds });
    expect(w.balance.amount).toBe("10.00");
    expect(w.version).toBe(1);
  });

  test("WIN credits, optionally referencing a BET of the same round", () => {
    const w = wallet("100.00");
    const bet = processed(K.Bet, "10.00", w);
    const win = tx(K.Win, "30.00", { referenceExternalTransactionId: bet.externalTransactionId });
    const out = settle(win, w, { reference: bet });
    expect(out.type).toBe("processed");
    expect(win.referenceTransactionId).toBe(bet.id);
    expect(w.balance.amount).toBe("120.00");
  });

  test("LOSS is processed without ledger and without version change", () => {
    const w = wallet("100.00");
    const t = tx(K.Loss, "10.00");
    const out = settle(t, w);
    expect(out.type === "processed" && out.entry).toBeUndefined();
    expect(t.status).toBe(S.Processed);
    expect(w.version).toBe(1);
  });

  test("REFUND reverses a processed BET once", () => {
    const w = wallet("100.00");
    const bet = processed(K.Bet, "40.00", w);
    const refund = tx(K.Refund, "40.00", { referenceExternalTransactionId: bet.externalTransactionId });
    expect(settle(refund, w, { reference: bet }).type).toBe("processed");
    expect(w.balance.amount).toBe("100.00");
    const again = tx(K.Refund, "40.00", { referenceExternalTransactionId: bet.externalTransactionId });
    expect(settle(again, w, { reference: bet, reversed: true })).toEqual({ type: "rejected", code: FailureCode.ReferenceAlreadyReversed });
  });

  test("REFUND only references BET", () => {
    const w = wallet();
    const win = processed(K.Win, "5.00", w);
    const refund = tx(K.Refund, "5.00", { referenceExternalTransactionId: win.externalTransactionId });
    expect(settle(refund, w, { reference: win })).toEqual({ type: "rejected", code: FailureCode.ReferenceKindNotAllowed });
  });

  test("ROLLBACK inverts BET, WIN and REFUND", () => {
    const w = wallet("100.00");
    const bet = processed(K.Bet, "30.00", w);
    const rbBet = tx(K.Rollback, "30.00", { referenceExternalTransactionId: bet.externalTransactionId });
    settle(rbBet, w, { reference: bet });
    expect(w.balance.amount).toBe("100.00");

    const win = processed(K.Win, "50.00", w);
    const rbWin = tx(K.Rollback, "50.00", { referenceExternalTransactionId: win.externalTransactionId });
    const out = settle(rbWin, w, { reference: win });
    expect(out.type === "processed" && out.entry?.direction).toBe(LedgerDirection.Debit);
    expect(w.balance.amount).toBe("100.00");

    const bet2 = processed(K.Bet, "20.00", w);
    const refund = tx(K.Refund, "20.00", { referenceExternalTransactionId: bet2.externalTransactionId });
    settle(refund, w, { reference: bet2 });
    const rbRefund = tx(K.Rollback, "20.00", { referenceExternalTransactionId: refund.externalTransactionId });
    settle(rbRefund, w, { reference: refund });
    expect(w.balance.amount).toBe("80.00");
  });

  test("ROLLBACK cannot reference LOSS or another ROLLBACK", () => {
    const w = wallet();
    const loss = processed(K.Loss, "1.00", w);
    const rb = tx(K.Rollback, "1.00", { referenceExternalTransactionId: loss.externalTransactionId });
    expect(settle(rb, w, { reference: loss })).toEqual({ type: "rejected", code: FailureCode.ReferenceKindNotAllowed });
  });

  test("reversal that would go negative is rejected with its own code", () => {
    const w = wallet("0.00");
    const win = processed(K.Win, "50.00", w);
    processed(K.Bet, "45.00", w); // balance 5.00
    const rb = tx(K.Rollback, "50.00", { referenceExternalTransactionId: win.externalTransactionId });
    expect(settle(rb, w, { reference: win })).toEqual({ type: "rejected", code: FailureCode.ReversalInsufficientFunds });
    expect(FailureCode.ReversalInsufficientFunds).not.toBe(FailureCode.InsufficientFunds);
    expect(w.balance.amount).toBe("5.00");
  });

  test("reversal amount must equal the reference amount", () => {
    const w = wallet();
    const bet = processed(K.Bet, "10.00", w);
    const refund = tx(K.Refund, "9.99", { referenceExternalTransactionId: bet.externalTransactionId });
    expect(settle(refund, w, { reference: bet })).toEqual({ type: "rejected", code: FailureCode.ReversalAmountMismatch });
  });

  test("reference must share provider, player, wallet, currency and round", () => {
    const w = wallet();
    const bet = processed(K.Bet, "10.00", w);
    const otherRound = tx(K.Refund, "10.00", { referenceExternalTransactionId: bet.externalTransactionId, roundId: "r2" });
    expect(settle(otherRound, w, { reference: bet })).toEqual({ type: "rejected", code: FailureCode.ReferenceScopeMismatch });
  });

  test("reference must be PROCESSED", () => {
    const w = wallet("1.00");
    const bet = tx(K.Bet, "10.00");
    settle(bet, w); // rejected: no funds
    const refund = tx(K.Refund, "10.00", { referenceExternalTransactionId: bet.externalTransactionId });
    expect(settle(refund, w, { reference: bet })).toEqual({ type: "rejected", code: FailureCode.ReferenceNotProcessed });
  });

  test("missing reference parks the transaction, exhaustion rejects REFERENCE_NOT_FOUND", () => {
    const w = wallet();
    const refund = tx(K.Refund, "10.00", { referenceExternalTransactionId: "not-yet" });
    expect(settle(refund, w).type).toBe("pending_reference");
    expect(refund.status).toBe(S.PendingReference);
    for (let i = 0; i < policy.maxAttempts; i++) expect(settle(refund, w).type).toBe("pending_reference");
    expect(settle(refund, w)).toEqual({ type: "rejected", code: FailureCode.ReferenceNotFound });
  });

  test("currency of the operation must match the wallet", () => {
    const w = wallet();
    const t = WagerTransaction.create({
      id: id(), providerId: "prov", externalTransactionId: "usd", idempotencyKey: "k", payloadHash: "h", walletId: "w1",
      playerId: "p1", roundId: "r1", gameId: "g", kind: K.Win, money: Money.from({ amount: "1.00", currency: "USD" }), createdAt: now,
    });
    expect(settle(t, w)).toEqual({ type: "rejected", code: FailureCode.CurrencyMismatch });
    expect(w.version).toBe(1);
  });

  test("player must own the wallet", () => {
    const w = wallet();
    expect(settle(tx(K.Bet, "1.00", { playerId: "intruder" }), w)).toEqual({ type: "rejected", code: FailureCode.WalletPlayerMismatch });
  });
});
