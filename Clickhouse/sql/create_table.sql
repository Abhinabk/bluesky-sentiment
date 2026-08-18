CREATE TABLE IF NOT EXISTS bluesky.mentions(
    brand LowCardinality(String),
    text String,
    sentiment LowCardinality(String),
    score Float32,
    ts DateTime64(6) -- jetstream uses micros_sec precision
)
ENGINE = MergeTree
ORDER BY (brand,ts)