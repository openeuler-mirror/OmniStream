SET sql-client.execution.result-mode=TABLEAU;

CREATE TABLE bid (
                     bidder BIGINT,
                     bid_count BIGINT,
                     dateTime TIMESTAMP(3),
                     WATERMARK FOR dateTime AS dateTime - INTERVAL '2' SECOND

) WITH (
      'connector' = 'filesystem',
      'path' = '/repo/codehub/OmniStream/testtool/csv_files/bid11.csv',
      'format' = 'csv',
      'csv.field-delimiter' = ',',
      'csv.ignore-parse-errors' = 'true'
      );

CREATE VIEW B AS SELECT *, PROCTIME() as p_time FROM bid;

SELECT
    bidder,
    count(*) as bid_count,
    window_start AS starttime,
    window_end AS endtime
FROM TABLE(
        TUMBLE(TABLE B, DESCRIPTOR(p_time), INTERVAL '10' SECOND))
GROUP BY bidder, window_start, window_end;