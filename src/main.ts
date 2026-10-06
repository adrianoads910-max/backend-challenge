import "reflect-metadata";
import { NestFactory } from "@nestjs/core";
import type { NestExpressApplication } from "@nestjs/platform-express";
import { createAppModule } from "./app.module";
import { loadConfig } from "./config";
import { PinoAppLogger } from "./infrastructure/observability/logger";

export async function bootstrap(env: Record<string, string | undefined> = process.env) {
  const config = loadConfig(env);
  const logger = new PinoAppLogger(config.LOG_LEVEL, { service: "wagering-processor", instance: config.INSTANCE_ID });
  const app = await NestFactory.create<NestExpressApplication>(createAppModule({ config, logger }), {
    logger,
    bufferLogs: false,
  });
  app.disable("x-powered-by");
  app.useBodyParser("json", { limit: "64kb" });
  app.enableShutdownHooks(["SIGTERM", "SIGINT"]);
  if (config.ENABLE_HTTP) await app.listen(config.PORT);
  else await app.init();
  logger.info("service started", { port: config.ENABLE_HTTP ? config.PORT : undefined });
  return app;
}

if (import.meta.main) {
  bootstrap().catch((err) => {
    console.error(JSON.stringify({ level: "fatal", message: "failed to start", err: String(err?.stack ?? err) }));
    process.exit(1);
  });
}
