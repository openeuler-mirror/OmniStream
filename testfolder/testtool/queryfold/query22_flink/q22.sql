SET sql-client.execution.result-mode=TABLEAU;
------------------------------------q22------------------------------------------------------------
CREATE TABLE bid (
                     auction  BIGINT,
                     bidder  BIGINT,
                     price  BIGINT,
                     dateTime TIMESTAMP(3),

                     channel  VARCHAR,
                     url  VARCHAR
) WITH (
      'connector' = 'filesystem',
      'path' = '/repo/codehub/testtool/csv_files/bid22.csv',
      'format' = 'csv',
      'csv.field-delimiter' = ',',
      'csv.ignore-parse-errors' = 'true'
      );

CREATE TABLE nexmark_q22 (
                             auction  BIGINT,
                             bidder  BIGINT,
                             price  BIGINT,
                             channel  VARCHAR,
                             dir1  VARCHAR,
                             dir2  VARCHAR,
                             dir3  VARCHAR
) WITH (
      'connector' = 'filesystem',
      'path' = '/repo/codehub/test_tool/test_flink/',
      'format' = 'csv',
      'csv.field-delimiter' = ',',
      'csv.ignore-parse-errors' = 'true'
      );

INSERT INTO nexmark_q22



SELECT
    auction, bidder, price, channel,
    SPLIT_INDEX(url, '/', 3) as dir1,
    SPLIT_INDEX(url, '/', 4) as dir2,
    SPLIT_INDEX(url, '/', 5) as dir3 FROM bid;
