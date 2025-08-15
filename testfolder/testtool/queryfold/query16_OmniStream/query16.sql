SET sql-client.execution.result-mode=TABLEAU;
CREATE TABLE bid(
                    auction BIGINT,
                    bidder BIGINT,
                    price BIGINT,
                    dateTime TIMESTAMP(3),
                    channel VARCHAR

) WITH (
      'connector' = 'filesystem',
      'path' = '/repo/codehub/OmniStream/testfolder/testtool/csv_files/bid16.csv',
--        'path' = '${CSV_PATH}',
      'format' = 'csv',
      'csv.field-delimiter' = ',',
      'csv.ignore-parse-errors' = 'true'
      );

SELECT
    channel,
    DATE_FORMAT(dateTime, 'yyyy-MM-dd') as `day`,
    max(DATE_FORMAT(dateTime, 'HH:mm')) as `minute`,
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
GROUP BY channel, DATE_FORMAT(dateTime, 'yyyy-MM-dd');
