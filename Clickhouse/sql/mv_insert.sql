CREATE MATERIALIZED VIEW IF NOT EXISTS bluesky.mentions_mv
TO bluesky.mentions
AS
SELECT
    brand,
    text,
    sentiment,
    score,
    fromUnixTimestamp64Micro(time_us) as ts 
FROM bluesky.mentions_queue;
