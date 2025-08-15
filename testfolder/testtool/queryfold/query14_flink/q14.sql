SET sql-client.execution.result-mode=TABLEAU;
------------------------------------q10------------------------------------------------------------
CREATE FUNCTION count_char AS 'com.github.nexmark.flink.udf.CountChar';

create table  bid(
                     auction BIGINT,
                     bidder  BIGINT,
                     price BIGINT,
                     channel VARCHAR,
                     url  VARCHAR,
                     dateTime TIMESTAMP(3),
                     extra VARCHAR
)WITH(
     'connector' = 'filesystem',
     'path' = '/repo/codehub/test_tool/bids_data.csv',
     'format' = 'csv',
     'csv.field-delimiter' = ',',
     'csv.ignore-parse-errors' = 'true'

     );


CREATE TABLE nexmark_q15 (
                             auction BIGINT,
                             bidder BIGINT,
                             price BIGINT,
                             bidTimeType varchar,
                             dateTime timestamp(3),
                             extra varchar
) WITH (
      'connector' = 'filesystem',
      'path' = '/repo/codehub/test_tool/test_flink/',
      'format' = 'csv',
      'csv.field-delimiter' = ',',
      'csv.ignore-parse-errors' = 'true'
      );

insert into nexmark_q15
SELECT
    auction,
    bidder,
    price,
    CASE
        WHEN HOUR(dateTime) >= 8 AND HOUR(dateTime) <= 18 THEN 'dayTime'
        WHEN HOUR(dateTime) <= 6 OR HOUR(dateTime) >= 20 THEN 'nightTime'
        ELSE 'otherTime'
END AS bidTimeType,
    dateTime,
    extra
--     count_char(extra, 'c') AS c_counts
FROM bid
WHERE 0.908 * price > 1000000 AND 0.908 * price < 50000000;