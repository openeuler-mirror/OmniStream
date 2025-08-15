SET sql-client.execution.result-mode=TABLEAU;
------------------------------------q10------------------------------------------------------------
-- CREATE TABLE nexmark_q15 (
--                              `day` VARCHAR,
--                              total_bids BIGINT,
--                              rank1_bids BIGINT,
--                              rank2_bids BIGINT,
--                              rank3_bids BIGINT,
--                              total_bidders BIGINT,
--                              rank1_bidders BIGINT,
--                              rank2_bidders BIGINT,
--                              rank3_bidders BIGINT,
--                              total_auctions BIGINT,
--                              rank1_auctions BIGINT,
--                              rank2_auctions BIGINT,
--                              rank3_auctions BIGINT
-- ) WITH (
--       'connector' = 'filesystem',
--       'path' = '/repo/codehub/test_tool/test_flink/',
--       'format' = 'csv',
--       'csv.field-delimiter' = ',',
--       'csv.ignore-parse-errors' = 'true'
--       );
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

SELECT
    DATE_FORMAT(dateTime, 'yyyy-MM-dd') as `day`,
    count(*) AS total_bids,
    count(*) filter (where price < 10000) AS rank1_bids,
        count(*) filter (where price >= 10000 and price < 1000000) AS rank2_bids,
        count(*) filter (where price >= 1000000) AS rank3_bids,
        count(distinct bidder) AS total_bidders,
    count(distinct bidder) filter (where price < 10000) AS rank1_bidders,
        count(distinct bidder) filter (where price >= 10000 and price < 1000000) AS rank2_bidders,
        count(distinct bidder) filter (where price >= 1000000) AS rank3_bidders,
        count(distinct auction) AS total_auctions,
    count(distinct auction) filter (where price < 10000) AS rank1_auctions,
        count(distinct auction) filter (where price >= 10000 and price < 1000000) AS rank2_auctions,
        count(distinct auction) filter (where price >= 1000000) AS rank3_auctions
FROM bid
GROUP BY DATE_FORMAT(dateTime, 'yyyy-MM-dd');