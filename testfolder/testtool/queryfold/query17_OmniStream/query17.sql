SET sql-client.execution.result-mode=TABLEAU;
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
     'path' = '${CSV_PATH}',
     'format' = 'csv',
     'csv.field-delimiter' = ',',
     'csv.ignore-parse-errors' = 'true'

     );

SELECT
    auction,
    DATE_FORMAT(dateTime, 'yyyy-MM-dd') as `day`,
    count(*) AS total_bids,
    count(*) filter (where price < 10000) AS rank1_bids,
        count(*) filter (where price >= 10000 and price < 1000000) AS rank2_bids,
        count(*) filter (where price >= 1000000) AS rank3_bids,
        min(price) AS min_price,
    max(price) AS max_price,
    avg(price) AS avg_price,
    sum(price) AS sum_price
FROM bid
GROUP BY auction, DATE_FORMAT(dateTime, 'yyyy-MM-dd');
