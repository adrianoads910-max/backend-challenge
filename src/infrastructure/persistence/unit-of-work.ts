import type { EntityManager, MikroORM } from "@mikro-orm/postgresql";
import { ApplicationError } from "../../application/errors";
import type { ReadRepositories, TransactionalRepositories, UnitOfWork } from "../../application/ports";
import { classifyTransient } from "./pg-errors";
import {
  PgLedgerRepository,
  PgWagerTransactionRepository,
  PgWalletRepository,
  transactionalRepositories,
} from "./repositories";

/**
 * One business operation = one `em.transactional()` (READ COMMITTED). Correctness comes from the
 * explicit row locks and constraints, not from the isolation level; lock_timeout bounds how long a
 * hot wallet can make a request wait before it is reported as a transient failure.
 */
export class MikroOrmUnitOfWork implements UnitOfWork {
  constructor(private readonly orm: MikroORM, private readonly lockTimeoutMs: number) {}

  async run<T>(work: (repos: TransactionalRepositories) => Promise<T>): Promise<T> {
    try {
      return await this.orm.em.fork().transactional(async (em: EntityManager) => {
        await em.execute(`set local lock_timeout = '${Math.trunc(this.lockTimeoutMs)}ms'`);
        return work(transactionalRepositories(em));
      });
    } catch (err) {
      if (err instanceof ApplicationError) throw err;
      throw classifyTransient(err) ?? err;
    }
  }
}

/** Read repositories on a fresh fork per call (no shared identity map between requests). */
export function readRepositories(orm: MikroORM): ReadRepositories {
  const fork = () => orm.em.fork();
  const wrap = async <T>(fn: () => Promise<T>) => {
    try {
      return await fn();
    } catch (err) {
      throw classifyTransient(err) ?? err;
    }
  };
  return {
    wallets: { findById: (id) => wrap(() => new PgWalletRepository(fork()).findById(id)) },
    transactions: {
      findById: (id) => wrap(() => new PgWagerTransactionRepository(fork()).findById(id)),
      findByExternalId: (p, e) => wrap(() => new PgWagerTransactionRepository(fork()).findByExternalId(p, e)),
      findDuePendingReferenceIds: (now, limit) =>
        wrap(() => new PgWagerTransactionRepository(fork()).findDuePendingReferenceIds(now, limit)),
    },
    ledger: { page: (w, a, l) => wrap(() => new PgLedgerRepository(fork()).page(w, a, l)) },
  };
}
