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
      'path' = '/repo/codehub/OmniStream/testfoler/testtool/csv_files/bid21.csv',
      'format' = 'csv',
      'csv.field-delimiter' = ',',
      'csv.ignore-parse-errors' = 'true'
      );



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
