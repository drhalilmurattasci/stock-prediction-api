CREATE EXTENSION IF NOT EXISTS timescaledb;
-- Example hypertable (uncomment once the bars table exists):
-- CREATE TABLE IF NOT EXISTS bars (
--   symbol text NOT NULL, ts timestamptz NOT NULL,
--   open double precision, high double precision, low double precision,
--   close double precision, volume double precision,
--   PRIMARY KEY (symbol, ts)
-- );
-- SELECT create_hypertable('bars', 'ts', if_not_exists => TRUE);
