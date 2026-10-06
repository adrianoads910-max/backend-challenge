import { Migration } from "@mikro-orm/migrations";

/**
 * Initial schema. Every financial guarantee of the challenge is enforced HERE, not only in code:
 *
 *  - uniqueness:      one wallet per (player, currency); one transaction per idempotency key and per
 *                     (provider, external id); at most one ledger entry per (transaction, wallet);
 *                     one ledger entry per wallet version; one successful reversal per reference.
 *  - non-negativity:  CHECKs on wallet balance and on every ledger balance / amount.
 *  - immutability:    ledger rejects UPDATE/DELETE/TRUNCATE; transactions in a terminal state cannot
 *                     change; business columns of a transaction can never change.
 *  - consistency:     deferred constraint triggers verify at COMMIT that the wallet balance equals the
 *                     balance_after of its latest ledger entry and that the ledger forms an unbroken
 *                     chain (entry N.balance_before == entry N-1.balance_after).
 */
export class Migration20261006000001_initial_schema extends Migration {
  override async up(): Promise<void> {
    this.addSql(`
create table wallets (
  id          uuid primary key,
  player_id   varchar(64)    not null,
  currency    char(3)        not null,
  balance     numeric(20, 2) not null,
  version     integer        not null,
  created_at  timestamptz    not null,
  updated_at  timestamptz    not null,
  constraint wallets_player_currency_uq unique (player_id, currency),
  constraint wallets_id_currency_uq     unique (id, currency),
  constraint wallets_balance_non_negative check (balance >= 0),
  constraint wallets_version_positive     check (version >= 1),
  constraint wallets_currency_iso         check (currency ~ '^[A-Z]{3}$')
);`);

    this.addSql(`
create table wager_transactions (
  id                                 uuid primary key,
  provider_id                        varchar(64)    not null,
  external_transaction_id            varchar(128)   not null,
  idempotency_key                    varchar(255)   not null,
  payload_hash                       char(64)       not null,
  wallet_id                          uuid           not null references wallets (id),
  player_id                          varchar(64)    not null,
  round_id                           varchar(128)   not null,
  game_id                            varchar(128)   not null,
  kind                               varchar(16)    not null,
  amount                             numeric(20, 2) not null,
  currency                           char(3)        not null,
  reference_external_transaction_id  varchar(128),
  reference_transaction_id           uuid references wager_transactions (id),
  status                             varchar(24)    not null,
  failure_code                       varchar(64),
  result_balance                     numeric(20, 2),
  reference_attempts                 integer        not null default 0,
  next_attempt_at                    timestamptz,
  processed_at                       timestamptz,
  created_at                         timestamptz    not null,
  updated_at                         timestamptz    not null,
  constraint wager_tx_idempotency_key_uq      unique (idempotency_key),
  constraint wager_tx_provider_external_id_uq unique (provider_id, external_transaction_id),
  constraint wager_tx_kind_valid   check (kind in ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
  constraint wager_tx_status_valid check (status in ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
  constraint wager_tx_amount_non_negative check (amount >= 0),
  constraint wager_tx_amount_positive_when_moving check (kind = 'LOSS' or amount > 0),
  constraint wager_tx_result_balance_non_negative check (result_balance is null or result_balance >= 0),
  constraint wager_tx_currency_iso check (currency ~ '^[A-Z]{3}$'),
  constraint wager_tx_opening_is_internal check ((kind = 'OPENING') = (provider_id = '__internal__')),
  constraint wager_tx_reversal_requires_reference
    check (kind not in ('REFUND','ROLLBACK') or reference_external_transaction_id is not null),
  constraint wager_tx_failure_code_iff_failed
    check ((status in ('REJECTED','FAILED')) = (failure_code is not null)),
  constraint wager_tx_terminal_has_processed_at
    check (status not in ('PROCESSED','REJECTED','FAILED') or processed_at is not null),
  constraint wager_tx_pending_reference_scheduled
    check (status <> 'PENDING_REFERENCE' or next_attempt_at is not null),
  constraint wager_tx_reference_attempts_non_negative check (reference_attempts >= 0)
);`);

    // A reference (BET / WIN / REFUND) can be successfully reversed only once, whatever the reversal kind.
    this.addSql(`
create unique index wager_tx_single_reversal_uq on wager_transactions (reference_transaction_id)
  where kind in ('REFUND','ROLLBACK') and status = 'PROCESSED';`);
    // Worker scan for out-of-order references, and wake-up when the reference arrives.
    this.addSql(`
create index wager_tx_pending_reference_due_idx on wager_transactions (next_attempt_at)
  where status = 'PENDING_REFERENCE';`);
    this.addSql(`
create index wager_tx_pending_reference_lookup_idx
  on wager_transactions (provider_id, reference_external_transaction_id)
  where status = 'PENDING_REFERENCE';`);
    this.addSql(`create index wager_tx_wallet_idx on wager_transactions (wallet_id, created_at);`);

    this.addSql(`
create table wallet_ledger_entries (
  id              uuid primary key,
  wallet_id       uuid           not null,
  currency        char(3)        not null,
  transaction_id  uuid           not null references wager_transactions (id),
  wallet_version  integer        not null,
  direction       varchar(6)     not null,
  amount          numeric(20, 2) not null,
  balance_before  numeric(20, 2) not null,
  balance_after   numeric(20, 2) not null,
  created_at      timestamptz    not null,
  -- composite FK: an entry can only exist in the wallet's own currency
  constraint ledger_wallet_currency_fk foreign key (wallet_id, currency) references wallets (id, currency),
  constraint ledger_one_entry_per_tx_wallet_uq unique (transaction_id, wallet_id),
  constraint ledger_wallet_version_uq          unique (wallet_id, wallet_version),
  constraint ledger_direction_valid  check (direction in ('DEBIT','CREDIT')),
  constraint ledger_amount_positive  check (amount > 0),
  constraint ledger_balances_non_negative check (balance_before >= 0 and balance_after >= 0),
  constraint ledger_wallet_version_positive check (wallet_version >= 1),
  constraint ledger_entry_balanced check (
    (direction = 'CREDIT' and balance_after = balance_before + amount) or
    (direction = 'DEBIT'  and balance_after = balance_before - amount)
  )
);`);

    this.addSql(`
create table inbox_messages (
  consumer_name varchar(128) not null,
  message_id    varchar(255) not null,
  payload_hash  char(64)     not null,
  received_at   timestamptz  not null,
  processed_at  timestamptz,
  primary key (consumer_name, message_id)
);`);

    this.addSql(`
create table outbox_messages (
  id              uuid primary key,
  seq             bigint generated always as identity,
  aggregate_id    varchar(64)  not null,
  event_type      varchar(128) not null,
  payload         jsonb        not null,
  occurred_at     timestamptz  not null,
  attempts        integer      not null default 0,
  next_attempt_at timestamptz,
  published_at    timestamptz,
  locked_by       varchar(128),
  locked_until    timestamptz,
  last_error      text,
  constraint outbox_attempts_non_negative check (attempts >= 0)
);`);
    this.addSql(`
create index outbox_pending_idx on outbox_messages (next_attempt_at, seq) where published_at is null;`);

    // ------------------------------------------------------------------ guard triggers
    this.addSql(`
create function forbid_mutation() returns trigger language plpgsql as $$
begin
  raise exception 'table % is append-only (% forbidden)', tg_table_name, tg_op
    using errcode = 'restrict_violation';
end $$;`);
    this.addSql(`
create trigger ledger_append_only before update or delete on wallet_ledger_entries
  for each row execute function forbid_mutation();`);
    this.addSql(`
create trigger ledger_no_truncate before truncate on wallet_ledger_entries
  for each statement execute function forbid_mutation();`);
    this.addSql(`
create trigger wallets_no_delete before delete on wallets
  for each row execute function forbid_mutation();`);
    this.addSql(`
create trigger wager_tx_no_delete before delete on wager_transactions
  for each row execute function forbid_mutation();`);

    this.addSql(`
create function wager_tx_guard_update() returns trigger language plpgsql as $$
begin
  if old.status in ('PROCESSED','REJECTED','FAILED') then
    raise exception 'wager transaction % is terminal (%)', old.id, old.status using errcode = 'restrict_violation';
  end if;
  if (new.provider_id, new.external_transaction_id, new.idempotency_key, new.payload_hash, new.wallet_id,
      new.player_id, new.round_id, new.game_id, new.kind, new.amount, new.currency,
      new.reference_external_transaction_id, new.created_at)
     is distinct from
     (old.provider_id, old.external_transaction_id, old.idempotency_key, old.payload_hash, old.wallet_id,
      old.player_id, old.round_id, old.game_id, old.kind, old.amount, old.currency,
      old.reference_external_transaction_id, old.created_at) then
    raise exception 'business columns of wager transaction % are immutable', old.id using errcode = 'restrict_violation';
  end if;
  if new.status = 'PENDING' then
    raise exception 'wager transaction % cannot go back to PENDING', old.id using errcode = 'restrict_violation';
  end if;
  return new;
end $$;`);
    this.addSql(`
create trigger wager_tx_guard before update on wager_transactions
  for each row execute function wager_tx_guard_update();`);

    this.addSql(`
create function wallets_guard_update() returns trigger language plpgsql as $$
begin
  if (new.id, new.player_id, new.currency, new.created_at) is distinct from (old.id, old.player_id, old.currency, old.created_at) then
    raise exception 'wallet identity columns are immutable' using errcode = 'restrict_violation';
  end if;
  if new.balance <> old.balance and new.version <> old.version + 1 then
    raise exception 'wallet % balance changed without version increment', old.id using errcode = 'restrict_violation';
  end if;
  if new.balance = old.balance and new.version <> old.version then
    raise exception 'wallet % version changed without balance change', old.id using errcode = 'restrict_violation';
  end if;
  return new;
end $$;`);
    this.addSql(`
create trigger wallets_guard before update on wallets
  for each row execute function wallets_guard_update();`);

    // Deferred (checked at COMMIT): wallet balance must match its latest ledger entry.
    this.addSql(`
create function wallets_check_ledger() returns trigger language plpgsql as $$
declare
  w record;
  latest record;
begin
  select balance, version into w from wallets where id = new.id;
  select wallet_version, balance_after into latest
    from wallet_ledger_entries where wallet_id = new.id order by wallet_version desc limit 1;
  if latest is null then
    if w.balance <> 0 then
      raise exception 'wallet % has balance % but no ledger entries', new.id, w.balance using errcode = 'check_violation';
    end if;
  elsif latest.wallet_version <> w.version or latest.balance_after <> w.balance then
    raise exception 'wallet % (v%, %) diverges from ledger (v%, %)',
      new.id, w.version, w.balance, latest.wallet_version, latest.balance_after using errcode = 'check_violation';
  end if;
  return null;
end $$;`);
    this.addSql(`
create constraint trigger wallets_ledger_consistency after insert or update on wallets
  deferrable initially deferred for each row execute function wallets_check_ledger();`);

    // Deferred: each entry chains from the previous one and belongs to a PROCESSED, balance-moving
    // transaction of the same wallet and amount.
    this.addSql(`
create function ledger_check_entry() returns trigger language plpgsql as $$
declare
  prev_after numeric;
  tx record;
begin
  select balance_after into prev_after from wallet_ledger_entries
    where wallet_id = new.wallet_id and wallet_version = new.wallet_version - 1;
  if prev_after is null and new.balance_before <> 0 then
    raise exception 'ledger entry % does not chain (no previous entry, balance_before %)', new.id, new.balance_before
      using errcode = 'check_violation';
  end if;
  if prev_after is not null and prev_after <> new.balance_before then
    raise exception 'ledger entry % does not chain (% <> %)', new.id, prev_after, new.balance_before
      using errcode = 'check_violation';
  end if;
  select wallet_id, status, kind, amount into tx from wager_transactions where id = new.transaction_id;
  if tx.wallet_id <> new.wallet_id or tx.status <> 'PROCESSED' or tx.kind = 'LOSS' or tx.amount <> new.amount then
    raise exception 'ledger entry % does not match a processed transaction', new.id using errcode = 'check_violation';
  end if;
  return null;
end $$;`);
    this.addSql(`
create constraint trigger ledger_entry_consistency after insert on wallet_ledger_entries
  deferrable initially deferred for each row execute function ledger_check_entry();`);
  }

  override async down(): Promise<void> {
    this.addSql(`drop table if exists outbox_messages;`);
    this.addSql(`drop table if exists inbox_messages;`);
    this.addSql(`drop table if exists wallet_ledger_entries;`);
    this.addSql(`drop table if exists wager_transactions;`);
    this.addSql(`drop table if exists wallets;`);
    this.addSql(`drop function if exists ledger_check_entry();`);
    this.addSql(`drop function if exists wallets_check_ledger();`);
    this.addSql(`drop function if exists wallets_guard_update();`);
    this.addSql(`drop function if exists wager_tx_guard_update();`);
    this.addSql(`drop function if exists forbid_mutation();`);
  }
}
