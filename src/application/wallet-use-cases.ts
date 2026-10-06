import { Inject, Injectable } from "@nestjs/common";
import { DomainError } from "../domain/errors";
import type { EventContext } from "../domain/events/integration-event";
import { WagerTransactionProcessed, WalletBalanceChanged } from "../domain/events/wagering-events";
import type { WalletLedgerEntry } from "../domain/ledger-entry";
import { Money, type MoneyProps } from "../domain/money";
import { OutboxMessage } from "../domain/outbox-message";
import { Wallet } from "../domain/wallet";
import { WagerTransaction } from "../domain/wager-transaction";
import { NotFoundError, ValidationError } from "./errors";
import { sha256Canonical } from "./payload-hash";
import {
  type AppLogger,
  CLOCK,
  type Clock,
  ID_GENERATOR,
  type IdGenerator,
  LOGGER,
  METRICS,
  READ_REPOSITORIES,
  type ReadRepositories,
  UNIT_OF_WORK,
  type UnitOfWork,
  type WageringMetrics,
} from "./ports";

export const INTERNAL_PROVIDER_ID = "__internal__";

export interface CreateWalletCommand {
  playerId: string;
  initialBalance: MoneyProps;
}

@Injectable()
export class CreateWallet {
  constructor(
    @Inject(UNIT_OF_WORK) private readonly uow: UnitOfWork,
    @Inject(CLOCK) private readonly clock: Clock,
    @Inject(ID_GENERATOR) private readonly ids: IdGenerator,
    @Inject(LOGGER) private readonly logger: AppLogger,
  ) {}

  /** Wallet + OPENING transaction + opening CREDIT + outbox events, in one SQL transaction. */
  async execute(cmd: CreateWalletCommand, correlationId: string): Promise<Wallet> {
    let initialBalance: Money;
    try {
      initialBalance = Money.from(cmd.initialBalance);
    } catch (err) {
      if (err instanceof DomainError) throw new ValidationError(err.message, { code: err.code });
      throw err;
    }
    const now = this.clock.now();
    const walletId = this.ids.next();
    const openingTxId = this.ids.next();
    const { wallet, openingEntry } = Wallet.open({
      id: walletId,
      playerId: cmd.playerId,
      initialBalance,
      openingTransactionId: openingTxId,
      openingEntryId: this.ids.next(),
      at: now,
    });

    await this.uow.run(async (repos) => {
      await repos.wallets.insert(wallet);
      if (!openingEntry) return;
      const opening = this.openingTransaction(openingTxId, wallet, initialBalance, now);
      opening.markProcessed(undefined, now, wallet.balance);
      await repos.transactions.insert(opening);
      await repos.ledger.insert(openingEntry);
      const ctx = (): EventContext => ({ eventId: this.ids.next(), correlationId, occurredAt: now });
      await repos.outbox.add([
        OutboxMessage.enqueue(WalletBalanceChanged.from(wallet, openingEntry as WalletLedgerEntry, ctx())),
        OutboxMessage.enqueue(WagerTransactionProcessed.from(opening, ctx())),
      ]);
    });
    this.logger.info("wallet opened", { walletId: wallet.id, currency: wallet.currency });
    return wallet;
  }

  private openingTransaction(id: string, wallet: Wallet, money: Money, now: Date): WagerTransaction {
    const externalTransactionId = `opening:${wallet.id}`;
    return WagerTransaction.createOpening({
      id,
      providerId: INTERNAL_PROVIDER_ID,
      externalTransactionId,
      idempotencyKey: `${INTERNAL_PROVIDER_ID}:${externalTransactionId}`,
      payloadHash: sha256Canonical({ walletId: wallet.id, money: money.toJSON(), kind: "OPENING" }),
      walletId: wallet.id,
      playerId: wallet.playerId,
      roundId: "opening",
      gameId: "opening",
      money,
      createdAt: now,
    });
  }
}

export interface ReconciliationReport {
  walletId: string;
  storedBalance: Money;
  calculatedBalance: Money;
  difference: Money;
  consistent: boolean;
  checkedEntries: number;
}

/**
 * Recomputes the balance from the ledger under a FOR SHARE lock (writers wait, so the stored
 * balance and the ledger are read at the same point). Never corrects anything: a divergence is
 * logged, counted and reported.
 */
@Injectable()
export class ReconcileWallet {
  constructor(
    @Inject(UNIT_OF_WORK) private readonly uow: UnitOfWork,
    @Inject(METRICS) private readonly metrics: WageringMetrics,
    @Inject(LOGGER) private readonly logger: AppLogger,
  ) {}

  async execute(walletId: string): Promise<ReconciliationReport> {
    const report = await this.uow.run(async (repos) => {
      const wallet = await repos.wallets.lockForShare(walletId);
      if (!wallet) throw new NotFoundError("WALLET_NOT_FOUND", `wallet ${walletId} not found`);
      const summary = await repos.ledger.summarize(walletId, wallet.currency);
      const calculated = summary.credits.subtract(summary.debits);
      const difference = wallet.balance.subtract(calculated);
      return {
        walletId,
        storedBalance: wallet.balance,
        calculatedBalance: calculated,
        difference,
        consistent: difference.isZero(),
        checkedEntries: summary.entries,
      };
    });
    this.metrics.reconciliation(report.consistent);
    if (!report.consistent) {
      this.logger.error("wallet reconciliation divergence", {
        walletId,
        difference: report.difference.amount,
        checkedEntries: report.checkedEntries,
      });
    }
    return report;
  }
}

/** Read side. Single statements, no explicit transaction needed. */
@Injectable()
export class WageringQueries {
  constructor(@Inject(READ_REPOSITORIES) private readonly read: ReadRepositories) {}

  async wallet(id: string): Promise<Wallet> {
    const wallet = await this.read.wallets.findById(id);
    if (!wallet) throw new NotFoundError("WALLET_NOT_FOUND", `wallet ${id} not found`);
    return wallet;
  }

  async ledger(walletId: string, cursor: string | undefined, limit: number) {
    await this.wallet(walletId);
    const afterVersion = cursor ? decodeCursor(cursor) : 0;
    const rows = await this.read.ledger.page(walletId, afterVersion, limit + 1);
    const items = rows.slice(0, limit);
    const last = items.at(-1);
    return { items, nextCursor: rows.length > limit && last ? encodeCursor(last.walletVersion) : null };
  }

  async transaction(id: string): Promise<WagerTransaction> {
    const tx = await this.read.transactions.findById(id);
    if (!tx) throw new NotFoundError("TRANSACTION_NOT_FOUND", `transaction ${id} not found`);
    return tx;
  }

  async transactionByExternalId(providerId: string, externalId: string): Promise<WagerTransaction> {
    const tx = await this.read.transactions.findByExternalId(providerId, externalId);
    if (!tx) throw new NotFoundError("TRANSACTION_NOT_FOUND", `transaction ${providerId}/${externalId} not found`);
    return tx;
  }
}

/** Opaque cursor: base64url of the last seen wallet version (ledger entries are 1:1 with versions). */
export function encodeCursor(walletVersion: number): string {
  return Buffer.from(JSON.stringify({ v: walletVersion }), "utf8").toString("base64url");
}

export function decodeCursor(cursor: string): number {
  try {
    const parsed = JSON.parse(Buffer.from(cursor, "base64url").toString("utf8")) as { v?: unknown };
    if (typeof parsed.v === "number" && Number.isSafeInteger(parsed.v) && parsed.v >= 0) return parsed.v;
  } catch {
    /* fallthrough */
  }
  throw new ValidationError("invalid cursor");
}

