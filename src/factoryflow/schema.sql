CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.machines (
    machine_id      TEXT PRIMARY KEY,
    machine_name    TEXT NOT NULL,
    machine_type    TEXT NOT NULL,
    install_date    DATE NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_file     TEXT NOT NULL,
    batch_id        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS raw.production_events (
    event_id            TEXT PRIMARY KEY,
    machine_id          TEXT NOT NULL REFERENCES raw.machines (machine_id),
    event_timestamp     TIMESTAMP NOT NULL,
    product_code        TEXT NOT NULL,
    quantity_produced   INTEGER NOT NULL,
    unit                TEXT NOT NULL,
    shift               TEXT NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_file         TEXT NOT NULL,
    batch_id            TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS raw.quality_checks (
    check_id                TEXT PRIMARY KEY,
    production_event_id     TEXT NOT NULL REFERENCES raw.production_events (event_id),
    check_timestamp          TIMESTAMP NOT NULL,
    result                   TEXT NOT NULL,
    defect_type              TEXT,
    inspector_id             TEXT,
    ingested_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_file              TEXT NOT NULL,
    batch_id                 TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS raw.maintenance_events (
    maintenance_id   TEXT PRIMARY KEY,
    machine_id       TEXT NOT NULL REFERENCES raw.machines (machine_id),
    start_time       TIMESTAMP NOT NULL,
    end_time         TIMESTAMP NOT NULL,
    maintenance_type TEXT NOT NULL,
    technician_id    TEXT NOT NULL,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_file      TEXT NOT NULL,
    batch_id         TEXT NOT NULL
);

-- toute ligne rejetée avant insertion atterrit ici, quelle que soit la raison
CREATE TABLE IF NOT EXISTS raw.quarantine (
    id              BIGSERIAL PRIMARY KEY,
    table_name      TEXT NOT NULL,
    row_reference   TEXT NOT NULL,
    reason          TEXT NOT NULL,
    raw_data        JSONB NOT NULL,
    source_file     TEXT NOT NULL,
    batch_id        TEXT NOT NULL,
    quarantined_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (table_name, row_reference, source_file)
);
