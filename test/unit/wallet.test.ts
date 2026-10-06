import { describe, expect, test } from "bun:test";
import { CurrencyMismatchError, InsufficientFundsError, InvariantViolationError } from "../../src/domain/errors";
import { LedgerDirection, WalletLedgerEntry } from "../../src/domain/ledger-entry";
import { Money } from "../../src/domain/money";
import { Wallet } from "../../src/domain/wallet";

const brl = (amount: string) => Money.from({ amount, currency: "BRL" });
const now = new Date("2026-01-01T00:00:00Z");
let seq = 0;
const id = () => `id-${++seq}`;

const open = (amount = "100.00") =>
  Wallet.open({ id: "w1", playerId: "p1", initialBalance: brl(amount), openingTransactionId: "tx-open", openingEntryId: id(), at: now });

describe("Wallet", () => {
  test("opens at version 1 with an opening CREDIT matching the balance", () => {
    const { wallet, openingEntry } = open("100.00");
    expect(wallet.version).toBe(1);
    expect(wallet.balance.amount).toBe("100.00");
    expect(openingEntry?.direction).toBe(LedgerDirection.Credit);
    expect(openingEntry?.walletVersion).toBe(1);
    expect(openingEntry?.balanceBefore.amount).toBe("0.00");
    expect(openingEntry?.balanceAfter.equals(wallet.balance)).toBe(true);
  });

  test("zero initial balance opens without ledger entry", () => {
    const { wallet, openingEntry } = open("0.00");
    expect(wallet.version).toBe(1);
    expect(openingEntry).toBeUndefined();
  });

  test("debit and credit move balance, bump version by one and return a balanced entry", () => {
    const { wallet } = open("100.00");
    const debit = wallet.debit({ entryId: id(), transactionId: "t1", money: brl("80.00"), at: now });
    expect(wallet.balance.amount).toBe("20.00");
    expect(wallet.version).toBe(2);
    expect(debit.walletVersion).toBe(2);
    expect(debit.isBalanced()).toBe(true);
    const credit = wallet.credit({ entryId: id(), transactionId: "t2", money: brl("5.50"), at: now });
    expect(wallet.balance.amount).toBe("25.50");
    expect(wallet.version).toBe(3);
    expect(credit.balanceBefore.amount).toBe("20.00");
  });

  test("never goes negative", () => {
    const { wallet } = open("100.00");
    expect(() => wallet.debit({ entryId: id(), transactionId: "t", money: brl("100.01"), at: now })).toThrow(InsufficientFundsError);
    expect(wallet.balance.amount).toBe("100.00");
    expect(wallet.version).toBe(1);
    wallet.debit({ entryId: id(), transactionId: "t", money: brl("100.00"), at: now });
    expect(wallet.balance.isZero()).toBe(true);
  });

  test("rejects operations in another currency", () => {
    const { wallet } = open();
    const usd = Money.from({ amount: "1.00", currency: "USD" });
    expect(() => wallet.credit({ entryId: id(), transactionId: "t", money: usd, at: now })).toThrow(CurrencyMismatchError);
    expect(wallet.version).toBe(1);
  });

  test("rejects zero movements (version only changes when the balance changes)", () => {
    const { wallet } = open();
    expect(() => wallet.credit({ entryId: id(), transactionId: "t", money: brl("0.00"), at: now })).toThrow(InvariantViolationError);
    expect(wallet.version).toBe(1);
  });

  test("rehydrate restores state without revalidating", () => {
    const w = Wallet.rehydrate({
      id: "w", playerId: "p", currency: "BRL", balance: brl("7.00"), version: 9, createdAt: now, updatedAt: now,
    });
    expect(w.version).toBe(9);
    expect(w.balance.amount).toBe("7.00");
  });
});

describe("WalletLedgerEntry", () => {
  const base = {
    id: "e", walletId: "w", transactionId: "t", walletVersion: 2, createdAt: now,
  };

  test("validates arithmetic in the factory", () => {
    expect(() =>
      WalletLedgerEntry.create({ ...base, direction: LedgerDirection.Debit, money: brl("10.00"), balanceBefore: brl("50.00"), balanceAfter: brl("41.00") }),
    ).toThrow(InvariantViolationError);
    const ok = WalletLedgerEntry.create({ ...base, direction: LedgerDirection.Debit, money: brl("10.00"), balanceBefore: brl("50.00"), balanceAfter: brl("40.00") });
    expect(ok.isBalanced()).toBe(true);
    expect(ok.signedAmount().amount).toBe("-10.00");
  });

  test("rejects non-positive amounts", () => {
    expect(() =>
      WalletLedgerEntry.create({ ...base, direction: LedgerDirection.Credit, money: brl("0.00"), balanceBefore: brl("1.00"), balanceAfter: brl("1.00") }),
    ).toThrow(InvariantViolationError);
  });

  test("is structurally immutable", () => {
    const e = WalletLedgerEntry.create({ ...base, direction: LedgerDirection.Credit, money: brl("1.00"), balanceBefore: brl("1.00"), balanceAfter: brl("2.00") });
    expect(Object.isFrozen(e)).toBe(true);
    expect(() => {
      (e as unknown as { walletId: string }).walletId = "x";
    }).toThrow();
  });
});
