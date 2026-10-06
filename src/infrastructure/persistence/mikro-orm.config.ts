import { defineConfig } from "@mikro-orm/postgresql";
import { Migrator } from "@mikro-orm/migrations";
import { ENTITY_SCHEMAS } from "./records";
import { Migration20261006000001_initial_schema } from "./migrations/Migration20261006000001_initial_schema";

export interface DatabaseSettings {
  url: string;
  poolMax: number;
  statementTimeoutMs: number;
  debug?: boolean;
}

export function mikroOrmConfig(db: DatabaseSettings) {
  return defineConfig({
    clientUrl: db.url,
    entities: ENTITY_SCHEMAS,
    extensions: [Migrator],
    discovery: { warnWhenNoEntities: true },
    forceUtcTimezone: true,
    // Explicit list instead of a glob: works the same from sources, a bundle or a compiled binary.
    migrations: {
      tableName: "mikro_orm_migrations",
      transactional: true,
      allOrNothing: true,
      migrationsList: [
        { name: "Migration20261006000001_initial_schema", class: Migration20261006000001_initial_schema },
      ],
    },
    pool: { min: 1, max: db.poolMax, acquireTimeoutMillis: 5_000 },
    driverOptions: { connection: { statement_timeout: db.statementTimeoutMs } },
    debug: db.debug ?? false,
    allowGlobalContext: false,
  });
}
