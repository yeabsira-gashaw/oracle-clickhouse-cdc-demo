CREATE DATABASE IF NOT EXISTS poc;

CREATE TABLE IF NOT EXISTS poc.customer_events
(
  id UInt64,
  name String,
  email String,
  updated_at String,
  is_deleted UInt8,
  version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY id;

CREATE TABLE IF NOT EXISTS poc.customer_kafka
(
  ID Nullable(String),
  NAME Nullable(String),
  EMAIL Nullable(String),
  UPDATED_AT Nullable(UInt64),
  __deleted Nullable(String)
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
  kafka_topic_list = 'oracle-cdc.APP.CUSTOMERS',
  kafka_group_name = 'clickhouse-customers-v2',
  kafka_format = 'JSONEachRow',
  kafka_num_consumers = 1,
  kafka_handle_error_mode = 'stream';

CREATE MATERIALIZED VIEW IF NOT EXISTS poc.customer_kafka_mv
TO poc.customer_events
AS SELECT
  assumeNotNull(toUInt64OrNull(ID)) AS id,
  ifNull(NAME, '') AS name,
  ifNull(EMAIL, '') AS email,
  toString(fromUnixTimestamp64Milli(toInt64(ifNull(UPDATED_AT, 0)))) AS updated_at,
  toUInt8(ifNull(__deleted, 'false') = 'true') AS is_deleted,
  toUInt64(_offset) AS version
FROM poc.customer_kafka
WHERE ID IS NOT NULL AND toUInt64OrNull(ID) IS NOT NULL;
