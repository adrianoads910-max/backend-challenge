import { Body, Controller, Get, Headers, HttpCode, Inject, Param, Post, Query, Res } from "@nestjs/common";
import type { Response } from "express";
import { ProcessWagerTransaction } from "../../application/process-wager-transaction";
import { CreateWallet, ReconcileWallet, WageringQueries } from "../../application/wallet-use-cases";
import { ValidationError } from "../../application/errors";
import { currentContext } from "../../infrastructure/observability/context";
import { createWalletBody, idempotencyKey, ledgerQuery, parse, toCommand, wagerTransactionBody } from "../contracts";
import {
  presentLedgerEntry,
  presentReconciliation,
  presentSubmission,
  presentTransaction,
  presentWallet,
} from "./presenters";
import { z } from "zod";

const uuidParam = (value: string, name: string) => {
  if (!z.uuid().safeParse(value).success) throw new ValidationError(`${name} must be a UUID`);
  return value;
};
const correlationId = () => currentContext().correlationId!;

@Controller("wallets")
export class WalletsController {
  constructor(
    @Inject(CreateWallet) private readonly createWallet: CreateWallet,
    @Inject(ReconcileWallet) private readonly reconcile: ReconcileWallet,
    @Inject(WageringQueries) private readonly queries: WageringQueries,
  ) {}

  @Post()
  @HttpCode(201)
  async create(@Body() body: unknown) {
    const cmd = parse(createWalletBody, body);
    const wallet = await this.createWallet.execute(cmd, correlationId());
    const { id, playerId, balance, version } = presentWallet(wallet);
    return { id, playerId, balance, version };
  }

  @Get(":walletId")
  async get(@Param("walletId") walletId: string) {
    return presentWallet(await this.queries.wallet(uuidParam(walletId, "walletId")));
  }

  @Get(":walletId/ledger")
  async ledger(@Param("walletId") walletId: string, @Query() query: unknown) {
    const { cursor, limit } = parse(ledgerQuery, query);
    const page = await this.queries.ledger(uuidParam(walletId, "walletId"), cursor, limit);
    return { items: page.items.map(presentLedgerEntry), nextCursor: page.nextCursor };
  }

  @Post(":walletId/reconciliation")
  @HttpCode(200)
  async reconcileWallet(@Param("walletId") walletId: string) {
    return presentReconciliation(await this.reconcile.execute(uuidParam(walletId, "walletId")));
  }
}

@Controller()
export class WageringController {
  constructor(
    @Inject(ProcessWagerTransaction) private readonly processor: ProcessWagerTransaction,
    @Inject(WageringQueries) private readonly queries: WageringQueries,
  ) {}

  @Post("wagering/transactions")
  async submit(
    @Headers("idempotency-key") key: string | undefined,
    @Body() body: unknown,
    @Res({ passthrough: true }) res: Response,
  ) {
    if (key === undefined) throw new ValidationError("Idempotency-Key header is required");
    const parsedKey = parse(idempotencyKey, key);
    const payload = parse(wagerTransactionBody, body);
    const result = await this.processor.execute(toCommand(payload, parsedKey), {
      source: "http",
      correlationId: correlationId(),
      causationId: parsedKey,
    });
    const { status, body: response } = presentSubmission(result);
    res.status(status);
    if (result.idempotentReplay) res.setHeader("Idempotent-Replay", "true");
    return response;
  }

  @Get("wagering/transactions/:transactionId")
  async byId(@Param("transactionId") id: string) {
    return presentTransaction(await this.queries.transaction(uuidParam(id, "transactionId")));
  }

  @Get("providers/:providerId/wagering/transactions/:externalTransactionId")
  async byExternalId(@Param("providerId") providerId: string, @Param("externalTransactionId") externalId: string) {
    return presentTransaction(await this.queries.transactionByExternalId(providerId, externalId));
  }
}
