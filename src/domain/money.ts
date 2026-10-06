import { CurrencyMismatchError, InvalidMoneyError } from "./errors";

/** Wire / persistence representation. `amount` is always a decimal string with scale 2. */
export interface MoneyProps {
  amount: string;
  currency: string;
}

const SCALE = 2;
const FACTOR = 100n;
const CURRENCY_RE = /^[A-Z]{3}$/;
// Plain positional decimal: optional sign, digits, optional fraction of 1..2 digits.
// Rejects "", " 1", "1e3", "Infinity", "NaN", "0x10", "1.", ".5", "1.234".
const AMOUNT_RE = /^(-)?(\d+)(?:\.(\d{1,2}))?$/;
// NUMERIC(20,2) upper bound — keeps every value representable in the database.
const MAX_MINOR = 10n ** 20n - 1n;

/**
 * Immutable monetary value. Internally an exact integer count of minor units (bigint),
 * so there is no floating point anywhere and no rounding: inputs with more than two
 * decimals are rejected instead of silently rounded.
 */
export class Money {
  private constructor(
    private readonly minor: bigint,
    public readonly currency: string,
  ) {}

  /**
   * Parses a contract value. Contract amounts are non-negative by definition;
   * use {@link Money.parseSigned} for internal values that may be negative.
   */
  static from(props: MoneyProps): Money {
    const money = Money.parseSigned(props);
    if (money.isNegative()) throw new InvalidMoneyError("amount must not be negative");
    return money;
  }

  /** Same validation as {@link from} but accepts a leading minus (e.g. a difference). */
  static parseSigned(props: MoneyProps): Money {
    if (props === null || typeof props !== "object") throw new InvalidMoneyError("money must be an object");
    const { amount, currency } = props;
    Money.assertCurrency(currency);
    if (typeof amount !== "string") throw new InvalidMoneyError("amount must be a decimal string");
    const match = AMOUNT_RE.exec(amount);
    if (!match) throw new InvalidMoneyError(`invalid amount "${amount}"`);
    const [, sign, int, frac = ""] = match;
    const minor = BigInt(int!) * FACTOR + BigInt(frac.padEnd(SCALE, "0"));
    return Money.ofMinor(sign ? -minor : minor, currency);
  }

  static zero(currency: string): Money {
    Money.assertCurrency(currency);
    return new Money(0n, currency);
  }

  /** Exact construction from minor units (cents). */
  static ofMinor(minor: bigint, currency: string): Money {
    Money.assertCurrency(currency);
    if (minor > MAX_MINOR || minor < -MAX_MINOR) throw new InvalidMoneyError("amount out of range");
    return new Money(minor === 0n ? 0n : minor, currency);
  }

  add(other: Money): Money {
    this.assertSameCurrency(other);
    return Money.ofMinor(this.minor + other.minor, this.currency);
  }

  subtract(other: Money): Money {
    this.assertSameCurrency(other);
    return Money.ofMinor(this.minor - other.minor, this.currency);
  }

  negate(): Money {
    return Money.ofMinor(-this.minor, this.currency);
  }

  isZero(): boolean {
    return this.minor === 0n;
  }

  isPositive(): boolean {
    return this.minor > 0n;
  }

  isNegative(): boolean {
    return this.minor < 0n;
  }

  isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.minor < other.minor;
  }

  equals(other: Money): boolean {
    return this.currency === other.currency && this.minor === other.minor;
  }

  /** Decimal string with fixed scale 2, e.g. "-0.05", "25.00". */
  get amount(): string {
    const negative = this.minor < 0n;
    const abs = negative ? -this.minor : this.minor;
    const int = abs / FACTOR;
    const frac = (abs % FACTOR).toString().padStart(SCALE, "0");
    return `${negative ? "-" : ""}${int}.${frac}`;
  }

  toJSON(): MoneyProps {
    return { amount: this.amount, currency: this.currency };
  }

  toString(): string {
    return `${this.amount} ${this.currency}`;
  }

  private assertSameCurrency(other: Money): void {
    if (other.currency !== this.currency) {
      throw new CurrencyMismatchError(`cannot combine ${this.currency} with ${other.currency}`);
    }
  }

  private static assertCurrency(currency: unknown): asserts currency is string {
    if (typeof currency !== "string" || !CURRENCY_RE.test(currency)) {
      throw new InvalidMoneyError(`invalid ISO-4217 currency "${String(currency)}"`);
    }
  }
}
