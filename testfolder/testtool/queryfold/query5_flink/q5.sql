SET sql-client.execution.result-mode=TABLEAU;
CREATE TABLE bid (
                     auction BIGINT,
                     bidder BIGINT,
                     price BIGINT,
                     dateTime TIMESTAMP(3),
                     extra VARCHAR,
                     WATERMARK FOR dateTime AS dateTime - INTERVAL '2' SECOND
) WITH (
                       'connector' = 'filesystem',
                       'path' = '/repo/codehub/OmniStream/testtool/test_flink/bid7.csv',
                       'format' = 'csv',
                       'csv.field-delimiter' = ',',
                       'csv.ignore-parse-errors' = 'true'
      );

SELECT AuctionBids.auction, AuctionBids.num
FROM (
         SELECT
             auction,
             count(*) AS num,
             window_start AS starttime,
             window_end AS endtime
         FROM TABLE(
                 HOP(TABLE bid, DESCRIPTOR(dateTime), INTERVAL '2' SECOND, INTERVAL '10' SECOND))
         GROUP BY auction, window_start, window_end
     ) AS AuctionBids
         JOIN (
    SELECT
        max(CountBids.num) AS maxn,
        CountBids.starttime,
        CountBids.endtime
    FROM (
             SELECT
                 count(*) AS num,
                 window_start AS starttime,
                 window_end AS endtime
             FROM TABLE(
                     HOP(TABLE bid, DESCRIPTOR(dateTime), INTERVAL '2' SECOND, INTERVAL '10' SECOND))
             GROUP BY auction, window_start, window_end
         ) AS CountBids
    GROUP BY CountBids.starttime, CountBids.endtime
) AS MaxBids
              ON AuctionBids.starttime = MaxBids.starttime AND
                 AuctionBids.endtime = MaxBids.endtime AND
                 AuctionBids.num >= MaxBids.maxn;