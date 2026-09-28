-- WattStream: power usage per meter, per minute.
-- Run in the Confluent Cloud SQL workspace (catalog: wattstream, database: wattstream-cluster),
-- one statement per cell.
--
-- Event time: $rowtime is a column Confluent Cloud adds to every table. It holds the Kafka
-- record timestamp, which producer.py sets to the reading's event_ts, so windows follow
-- event time (when the meter took the reading), not processing time (when Flink sees it).
-- Watermark (Confluent Cloud default): the newest $rowtime seen in each partition minus
-- 180 ms. A window is emitted once the watermark passes its end. Partitions with no data
-- are marked idle after about 10 seconds, so they don't hold the watermark back.

-- 1) Create the output table and start the continuous job in one statement (CTAS).
--    This also creates the Kafka topic power-usage-1m and registers its Avro schema.
CREATE TABLE `power-usage-1m` AS
SELECT
  window_start,
  window_end,
  device_id,
  ROUND(AVG(power_w), 1)      AS avg_power_w,
  ROUND(MAX(power_w), 1)      AS max_power_w,
  -- Energy = average power x time. One minute is 1/60 hour, so Wh = average W / 60.
  ROUND(AVG(power_w) / 60, 2) AS energy_wh,
  COUNT(*)                    AS reading_count
-- TUMBLE splits event time into fixed, non-overlapping 1-minute windows.
FROM TABLE(
  TUMBLE(TABLE `meter-readings`, DESCRIPTOR($rowtime), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, device_id;

-- 2) Look at the results: one row per meter per minute, shortly after each minute ends.
--    Stop this query when you're done; the job from step 1 keeps running.
SELECT * FROM `power-usage-1m`;
