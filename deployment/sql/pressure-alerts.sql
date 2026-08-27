SET 'execution.runtime-mode' = 'streaming';
SET 'parallelism.default' = '1';

CREATE TABLE telemetry_source (
    tag STRING,
    `timestamp` STRING,
    `value` DOUBLE
)
WITH (
    'connector' = 'kafka',
    'topic' = 'telemetry',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'monitor',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'false'
);

CREATE TABLE pressure_event_sink (
    tag STRING,
    event_timestamp STRING,
    measured_value DOUBLE,
    threshold DOUBLE,
    severity STRING,
    event_type STRING,
    message STRING
)
WITH (
    'connector' = 'kafka',
    'topic' = 'events',
    'properties.bootstrap.servers' = 'kafka:29092',
    'format' = 'json',
    'sink.delivery-guarantee' = 'at-least-once'
);

INSERT INTO pressure_event_sink
SELECT
    tag,
    `timestamp`,
    `value`,
    CAST(100.0 AS DOUBLE),
    CASE
        WHEN `value` >= 150.0 THEN 'CRITICAL'
        ELSE 'WARNING'
    END,
    'PRESSURE_HIGH',
    CONCAT(
        'Pressure exceeded threshold: value=',
        CAST(`value` AS STRING),
        ', threshold=100.0'
    )
FROM telemetry_source
WHERE tag = 'pressure'
  AND `value` > 100.0;