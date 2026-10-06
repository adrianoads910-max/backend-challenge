import { z } from "zod";
import { ValidationError } from "../application/errors";
import type { SubmitWagerTransactionCommand } from "../application/process-wager-transaction";
import { SUBMITTABLE_KINDS } from "../domain/wager-transaction";

// Money stays a string end to end. Detailed rules (scale, sign, ISO code) live in Money.from.
const money = z.object({ amount: z.string(), currency: z.string() }).strict();
const id = (max: number) => z.string().min(1).max(max).regex(/^[A-Za-z0-9._:\-]+$/, "unsupported characters");

export const wagerTransactionBody = z
  .object({
    providerId: id(64).refine((v) => !v.startsWith("__"), "reserved provider id"),
    externalTransactionId: id(128),
    playerId: id(64),
    walletId: z.uuid(),
    roundId: id(128),
    gameId: id(128),
    kind: z.enum(SUBMITTABLE_KINDS),
    money,
    referenceExternalTransactionId: id(128).optional(),
  })
  .strict();

export const idempotencyKey = z.string().min(1).max(255).regex(/^[\x21-\x7E]+$/, "printable ASCII only");

export const createWalletBody = z.object({ playerId: id(64), initialBalance: money }).strict();

export const wagerMessage = z.object({
  messageId: z.string().min(1).max(255),
  type: z.literal("WagerTransactionRequested"),
  occurredAt: z.iso.datetime(),
  data: wagerTransactionBody.extend({ idempotencyKey }),
});

export const ledgerQuery = z.object({
  cursor: z.string().max(200).optional(),
  limit: z.coerce.number().int().min(1).max(200).default(50),
});

export function parse<T extends z.ZodType>(schema: T, input: unknown): z.infer<T> {
  const result = schema.safeParse(input);
  if (!result.success) {
    throw new ValidationError("invalid payload", {
      issues: result.error.issues.map((i) => ({ path: i.path.join("."), message: i.message })),
    });
  }
  return result.data;
}

export function toCommand(
  body: z.infer<typeof wagerTransactionBody>,
  key: string,
): SubmitWagerTransactionCommand {
  return { ...body, idempotencyKey: key };
}
