-- WattStream: overload alerts.
-- Run in the Confluent Cloud SQL workspace (catalog: wattstream, database: wattstream-cluster),
-- one statement per cell.

-- 1) Create the output table and start the continuous job in one statement (CTAS).
--    This also creates the Kafka topic overload-alerts and registers the Avro schema
--    that alert_consumer.py uses. It filters single rows, so there's no window or
--    watermark to wait for: each alert is written as soon as its reading arrives.
CREATE TABLE `overload-alerts` AS
SELECT
  device_id,
  event_ts,
  power_w,
  voltage_v,
  current_a,
  -- All labels are 4 letters on purpose: Flink types string literals as CHAR(n),
  -- so mixing lengths in one CASE can pad the shorter labels with spaces.
  CASE
    WHEN power_w >= 5000 THEN 'CRIT'
    WHEN power_w >= 4000 THEN 'HIGH'
    ELSE 'WARN'
  END AS severity
FROM `meter-readings`
WHERE power_w > 3000;

-- 2) Look at the results. Stop this query when you're done; the job keeps running.
SELECT * FROM `overload-alerts`;
