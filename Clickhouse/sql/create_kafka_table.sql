CREATE TABLE IF NOT EXISTS bluesky.mentions_queue(
    brand String,
    text String,
    sentiment String, 
    score Float32,
    time_us UInt64
)
ENGINE = Kafka 
SETTINGS 
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'posts.scored',
    kafka_group_name = 'clickhouse_mentions',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;