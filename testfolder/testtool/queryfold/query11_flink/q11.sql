CREATE TABLE bid (
                     bidder BIGINT,
                     bid_count BIGINT,
                     dateTime TIMESTAMP(3),
) WITH (
      'connector' = 'filesystem',
      'path' = '/repo/codehub/OmniStream/testtool/csv_files/bid11.csv',
      'format' = 'csv',
      'csv.field-delimiter' = ',',
      'csv.ignore-parse-errors' = 'true'
      );

SELECT
    B.bidder,
    count(*) as bid_count,
    SESSION_START(B.dateTime, INTERVAL '10' SECOND) as starttime,
    SESSION_END(B.dateTime, INTERVAL '10' SECOND) as endtime
FROM bid B
GROUP BY B.bidder, SESSION(B.dateTime, INTERVAL '10' SECOND);