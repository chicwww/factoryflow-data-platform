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
    event_timestamp      TIMESTAMP NOT NULL,
    product_code        TEXT NOT NULL,
    quantity_produced   INTEGER NOT NULL CHECK (quantity_produced > 0),
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

-- Anomaly scores are intentionally kept separate from the dbt marts: they
-- are the output of a statistical model, not a verified business fact, and
-- should never be silently blended into reporting tables. One row per
-- machine-day (matches the grain of mart_production_daily).
CREATE TABLE IF NOT EXISTS raw.anomaly_scores (
    machine_day_key             TEXT PRIMARY KEY,
    machine_id                  TEXT NOT NULL,
    production_date             DATE NOT NULL,
    total_quantity               INTEGER,
    baseline_mean                DOUBLE PRECISION,
    baseline_stddev              DOUBLE PRECISION,
    baseline_z_score             DOUBLE PRECISION,
    is_baseline_anomaly          BOOLEAN NOT NULL DEFAULT false,
    isolation_forest_score       DOUBLE PRECISION,
    is_isolation_forest_anomaly  BOOLEAN NOT NULL DEFAULT false,
    scored_at                    TIMESTAMPTZ NOT NULL DEFAULT now()
);
