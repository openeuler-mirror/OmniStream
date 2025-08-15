SET sql-client.execution.result-mode=TABLEAU;
------------------------------------q22------------------------------------------------------------
CREATE TABLE bid (

                     auction  BIGINT,
                     bidder  BIGINT,
                     price  BIGINT,

                     channel  VARCHAR,
                     url  VARCHAR,
                     dateTime TIMESTAMP(3),
                     extra VARCHAR
) WITH (
      'connector' = 'filesystem',
      'path' = '/repo/codehub/testtool/csv_files/bid21.csv',
      'format' = 'csv',
      'csv.field-delimiter' = ',',
      'csv.ignore-parse-errors' = 'true'
      );

CREATE TABLE nexmark_q22 (
                             auction  BIGINT,
                             bidder  BIGINT,
                             price  BIGINT,
                             channel  VARCHAR,
                             channel_id  VARCHAR
) WITH (
      'connector' = 'filesystem',
      'path' = '/repo/codehub/testtool/test_flink/',
      'format' = 'csv',
      'csv.field-delimiter' = ',',
      'csv.ignore-parse-errors' = 'true'
      );

INSERT INTO nexmark_q22



SELECT
    auction, bidder, price, channel,
    CASE
        WHEN lower(channel) = 'apple' THEN '0'
        WHEN lower(channel) = 'google' THEN '1'
        WHEN lower(channel) = 'facebook' THEN '2'
        WHEN lower(channel) = 'baidu' THEN '3'
        ELSE REGEXP_EXTRACT(url, '(&|^)channel_id=([^&]*)', 2)
        END
        AS channel_id FROM bid
where REGEXP_EXTRACT(url, '(&|^)channel_id=([^&]*)', 2) is not null or
    lower(channel) in ('apple', 'google', 'facebook', 'baidu');