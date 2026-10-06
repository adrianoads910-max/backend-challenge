import { MikroORM } from "@mikro-orm/postgresql";
import { loadConfig } from "./config";
import { mikroOrmConfig } from "./infrastructure/persistence/mikro-orm.config";

/** Usage: bun src/migrate.ts [up|down|pending]   (down reverts the last migration) */
export async function migrate(command: "up" | "down" | "pending" = "up", url?: string) {
  const config = loadConfig();
  const orm = await MikroORM.init(
    mikroOrmConfig({ url: url ?? config.DATABASE_URL, poolMax: 2, statementTimeoutMs: 60_000 }),
  );
  try {
    const migrator = orm.getMigrator();
    if (command === "up") return await migrator.up();
    if (command === "down") return await migrator.down();
    return await migrator.getPendingMigrations();
  } finally {
    await orm.close(true);
  }
}

if (import.meta.main) {
  const command = (process.argv[2] ?? "up") as "up" | "down" | "pending";
  for (let attempt = 1; ; attempt++) {
    try {
      const result = await migrate(command);
      console.log(JSON.stringify({ level: "info", message: `migrations ${command}`, result }));
      break;
    } catch (err) {
      if (attempt >= 20 || !/ECONNREFUSED|starting up|Connection terminated/i.test(String(err))) {
        console.error(err);
        process.exit(1);
      }
      await new Promise((r) => setTimeout(r, 1_000));
    }
  }
}
