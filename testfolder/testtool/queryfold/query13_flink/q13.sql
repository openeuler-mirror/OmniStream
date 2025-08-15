SET sql-client.execution.result-mode=TABLEAU;
------------------------------------q10------------------------------------------------------------
-- CREATE FUNCTION count_char AS 'com.github.nexmark.flink.udf.CountChar';
CREATE TABLE side_input (
                            key BIGINT,
                            `value` VARCHAR
) WITH (
    'connector.type' = 'filesystem',
    'connector.path' = '/repo/codehub/flink-1.16.3/data/side_input.txt',
    'format.type' = 'csv',
    'csv.field-delimiter' = ',',
    'csv.ignore-parse-errors' = 'true'
);

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
     'path' = '/repo/codehub/OmniStream/testtool/csv_files/bids_data.csv',
     'format' = 'csv',
     'csv.field-delimiter' = ',',
     'csv.ignore-parse-errors' = 'true'

     );


-- CREATE TABLE nexmark_q15 (
--                              auction BIGINT,
--                              bidder BIGINT,
--                              price BIGINT,
--                              bidTimeType varchar,
--                              dateTime timestamp(3),
--                              extra varchar
-- ) WITH (
--       'connector' = 'filesystem',
--       'path' = '/repo/codehub/test_tool/test_flink/',
--       'format' = 'csv',
--       'csv.field-delimiter' = ',',
--       'csv.ignore-parse-errors' = 'true'
--       );
--
-- insert into nexmark_q15
SELECT
    B.auction,
    B.bidder,
    B.price,
    B.dateTime,
    S.`value`
FROM (SELECT *, PROCTIME() as p_time FROM bid) B
         JOIN side_input FOR SYSTEM_TIME AS OF B.p_time AS S
              ON mod(B.auction, 10000) = S.key;