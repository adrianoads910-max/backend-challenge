import type { SQSClient } from "@aws-sdk/client-sqs";
import { MikroORM } from "@mikro-orm/postgresql";
import {
  Inject,
  type MiddlewareConsumer,
  Module,
  type NestModule,
  type OnApplicationBootstrap,
  type OnApplicationShutdown,
} from "@nestjs/common";
import { APP_FILTER, APP_GUARD } from "@nestjs/core";
import {
  CLOCK,
  ID_GENERATOR,
  LOGGER,
  METRICS,
  READ_REPOSITORIES,
  REFERENCE_RETRY_POLICY,
  UNIT_OF_WORK,
} from "./application/ports";
import { ProcessWagerTransaction } from "./application/process-wager-transaction";
import { ResolvePendingReference } from "./application/resolve-pending-reference";
import { CreateWallet, ReconcileWallet, WageringQueries } from "./application/wallet-use-cases";
import { APP_CONFIG, type AppConfig } from "./config";
import type { ReferenceRetryPolicy } from "./domain/wager-transaction";
import {
  PROVIDER_IDENTITY,
  ProviderAuthGuard,
  TrustAllProviderIdentity,
} from "./infrastructure/auth/provider-auth.guard";
import { OutboxPublisher } from "./infrastructure/messaging/outbox-publisher";
import { createSqsClient, type QueueUrls, resolveQueues } from "./infrastructure/messaging/sqs";
import { WagerConsumer } from "./infrastructure/messaging/wager-consumer";
import { PinoAppLogger } from "./infrastructure/observability/logger";
import { PromMetrics } from "./infrastructure/observability/metrics";
import { mikroOrmConfig } from "./infrastructure/persistence/mikro-orm.config";
import { MikroOrmUnitOfWork, readRepositories } from "./infrastructure/persistence/unit-of-work";
import { PendingReferenceWorker } from "./infrastructure/workers/pending-reference-worker";
import { WageringController, WalletsController } from "./interface/http/controllers";
import { correlationMiddleware } from "./interface/http/correlation.middleware";
import { HttpErrorFilter } from "./interface/http/error-filter";
import { HealthController, ORM, PROM_METRICS, QUEUE_URLS, SQS_CLIENT } from "./interface/http/health.controller";

export interface AppDependencies {
  config: AppConfig;
  logger: PinoAppLogger;
}

/** Composition root: wires ports to adapters. Everything framework-specific stays here. */
export function createAppModule({ config, logger }: AppDependencies) {
  const metrics = new PromMetrics();

  @Module({
    controllers: [WalletsController, WageringController, HealthController],
    providers: [
      { provide: APP_CONFIG, useValue: config },
      { provide: LOGGER, useValue: logger },
      { provide: METRICS, useValue: metrics },
      { provide: PROM_METRICS, useValue: metrics },
      { provide: CLOCK, useValue: { now: () => new Date() } },
      { provide: ID_GENERATOR, useValue: { next: () => Bun.randomUUIDv7() } },
      {
        provide: REFERENCE_RETRY_POLICY,
        useValue: {
          maxAttempts: config.PENDING_REFERENCE_MAX_ATTEMPTS,
          baseDelayMs: config.PENDING_REFERENCE_BASE_DELAY_MS,
          maxDelayMs: config.PENDING_REFERENCE_MAX_DELAY_MS,
        } satisfies ReferenceRetryPolicy,
      },
      {
        provide: ORM,
        useFactory: () =>
          MikroORM.init(
            mikroOrmConfig({
              url: config.DATABASE_URL,
              poolMax: config.DB_POOL_MAX,
              statementTimeoutMs: config.DB_STATEMENT_TIMEOUT_MS,
            }),
          ),
      },
      { provide: UNIT_OF_WORK, useFactory: (orm: MikroORM) => new MikroOrmUnitOfWork(orm, config.DB_LOCK_TIMEOUT_MS), inject: [ORM] },
      { provide: READ_REPOSITORIES, useFactory: (orm: MikroORM) => readRepositories(orm), inject: [ORM] },
      { provide: SQS_CLIENT, useFactory: () => createSqsClient(config) },
      {
        provide: QUEUE_URLS,
        useFactory: async (sqs: SQSClient) => {
          for (let attempt = 1; ; attempt++) {
            try {
              return await resolveQueues(sqs, config);
            } catch (err) {
              if (attempt >= 30) throw err;
              logger.warn("waiting for SQS", { attempt });
              await new Promise((r) => setTimeout(r, 1_000));
            }
          }
        },
        inject: [SQS_CLIENT],
      },
      { provide: PROVIDER_IDENTITY, useClass: TrustAllProviderIdentity },
      { provide: APP_GUARD, useClass: ProviderAuthGuard },
      { provide: APP_FILTER, useClass: HttpErrorFilter },
      ProcessWagerTransaction,
      ResolvePendingReference,
      CreateWallet,
      ReconcileWallet,
      WageringQueries,
      {
        provide: WagerConsumer,
        useFactory: (sqs: SQSClient, queues: QueueUrls, processor: ProcessWagerTransaction) =>
          new WagerConsumer(sqs, queues, processor, config, logger, metrics),
        inject: [SQS_CLIENT, QUEUE_URLS, ProcessWagerTransaction],
      },
      {
        provide: OutboxPublisher,
        useFactory: (orm: MikroORM, sqs: SQSClient, queues: QueueUrls) =>
          new OutboxPublisher(orm, sqs, queues.events, config, logger, metrics, config.INSTANCE_ID),
        inject: [ORM, SQS_CLIENT, QUEUE_URLS],
      },
      {
        provide: PendingReferenceWorker,
        useFactory: (resolver: ResolvePendingReference) => new PendingReferenceWorker(resolver, config, logger),
        inject: [ResolvePendingReference],
      },
    ],
  })
  class AppModule implements NestModule, OnApplicationBootstrap, OnApplicationShutdown {
    constructor(
      @Inject(WagerConsumer) private readonly consumer: WagerConsumer,
      @Inject(OutboxPublisher) private readonly publisher: OutboxPublisher,
      @Inject(PendingReferenceWorker) private readonly pendingWorker: PendingReferenceWorker,
      @Inject(ORM) private readonly orm: MikroORM,
    ) {}

    configure(consumer: MiddlewareConsumer) {
      consumer.apply(correlationMiddleware).forRoutes("*");
    }

    onApplicationBootstrap() {
      if (config.ENABLE_CONSUMER) this.consumer.start();
      if (config.ENABLE_OUTBOX_PUBLISHER) this.publisher.start();
      if (config.ENABLE_PENDING_REFERENCE_WORKER) this.pendingWorker.start();
    }

    /** SIGTERM: stop intake first, let in-flight work finish, then close the pool. */
    async onApplicationShutdown(signal?: string) {
      logger.info("shutting down", { signal });
      await Promise.allSettled([this.consumer.stop(), this.publisher.stop(), this.pendingWorker.stop()]);
      await this.orm.close(true);
      logger.info("shutdown complete");
    }
  }

  return AppModule;
}
