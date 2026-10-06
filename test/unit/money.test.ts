import { describe, expect, test } from "bun:test";
import { CurrencyMismatchError, InvalidMoneyError } from "../../src/domain/errors";
import { Money } from "../../src/domain/money";

const brl = (amount: string) => Money.from({ amount, currency: "BRL" });

describe("Money", () => {
  test("serialises with fixed scale 2", () => {
    expect(brl("25").toJSON()).toEqual({ amount: "25.00", currency: "BRL" });
    expect(brl("25.5").amount).toBe("25.50");
    expect(brl("0.07").amount).toBe("0.07");
    expect(brl("0").amount).toBe("0.00");
    expect(brl("1000000.10").toString()).toBe("1000000.10 BRL");
  });

  test("arithmetic is exact (no binary floating point)", () => {
    expect(brl("0.10").add(brl("0.20")).equals(brl("0.30"))).toBe(true);
    expect(brl("1.00").subtract(brl("0.99")).amount).toBe("0.01");
    // values beyond Number.MAX_SAFE_INTEGER cents stay exact
    expect(brl("99999999999999999.99").subtract(brl("0.01")).amount).toBe("99999999999999999.98");
  });

  test("never rounds: more than two decimals is rejected", () => {
    for (const amount of ["1.005", "0.001", "10.999"]) {
      expect(() => brl(amount)).toThrow(InvalidMoneyError);
    }
  });

  test.each([
    ["NaN"],
    ["Infinity"],
    ["-Infinity"],
    ["1e3"],
    ["1E-2"],
    [""],
    [" 1.00"],
    ["1.00 "],
    ["1,00"],
    ["+1.00"],
    [".5"],
    ["1."],
    ["0x10"],
    ["--1"],
  ])("rejects invalid amount %p", (amount) => {
    expect(() => brl(amount)).toThrow(InvalidMoneyError);
  });

  test("rejects non-string amounts (numbers never enter the domain)", () => {
    expect(() => Money.from({ amount: 25 as unknown as string, currency: "BRL" })).toThrow(InvalidMoneyError);
    expect(() => Money.from({ amount: null as unknown as string, currency: "BRL" })).toThrow(InvalidMoneyError);
  });

  test("rejects negative values in contracts but supports them internally", () => {
    expect(() => brl("-1.00")).toThrow(InvalidMoneyError);
    const diff = Money.parseSigned({ amount: "-1.50", currency: "BRL" });
    expect(diff.isNegative()).toBe(true);
    expect(diff.amount).toBe("-1.50");
    expect(diff.negate().amount).toBe("1.50");
    expect(brl("1.00").subtract(brl("1.01")).amount).toBe("-0.01");
  });

  test("rejects invalid currencies", () => {
    for (const currency of ["brl", "BR", "BRLL", "", "R$"]) {
      expect(() => Money.from({ amount: "1.00", currency })).toThrow(InvalidMoneyError);
    }
  });

  test("operations across currencies fail with a domain error", () => {
    const usd = Money.from({ amount: "1.00", currency: "USD" });
    expect(() => brl("1.00").add(usd)).toThrow(CurrencyMismatchError);
    expect(() => brl("1.00").subtract(usd)).toThrow(CurrencyMismatchError);
    expect(() => brl("1.00").isLessThan(usd)).toThrow(CurrencyMismatchError);
    expect(brl("1.00").equals(usd)).toBe(false);
  });

  test("is immutable", () => {
    const a = brl("10.00");
    const b = a.add(brl("5.00"));
    expect(a.amount).toBe("10.00");
    expect(b.amount).toBe("15.00");
    expect(b).not.toBe(a);
  });

  test("predicates", () => {
    expect(Money.zero("BRL").isZero()).toBe(true);
    expect(brl("0.01").isPositive()).toBe(true);
    expect(brl("0.00").isPositive()).toBe(false);
    expect(brl("1.00").isLessThan(brl("1.01"))).toBe(true);
    expect(Money.parseSigned({ amount: "-0.00", currency: "BRL" }).isNegative()).toBe(false);
  });
});
