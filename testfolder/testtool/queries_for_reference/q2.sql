-- -------------------------------------------------------------------------------------------------
-- Query2: Selection
-- -------------------------------------------------------------------------------------------------
-- Find bids with specific auction ids and show their bid price.
--
-- In original Nexmark queries, Query2 is as following (in CQL syntax):
--
--   SELECT Rstream(auction, price)
--   FROM Bid [NOW]
--   WHERE auction = 1007 OR auction = 1020 OR auction = 2001 OR auction = 2019 OR auction = 2087;
--
-- However, that query will only yield a few hundred results over event streams of arbitrary size.
-- To make it more interesting we instead choose bids for every 123'th auction.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE bid (
  auction  BIGINT,
  price  BIGINT
) WITH (
                       'connector' = 'filesystem',
                       'path' = '/repo/codehub/OmniStream/testtool/csv_files/bid_q2.csv',
                       'format' = 'csv',
                       'csv.field-delimiter' = ',',
                       'csv.ignore-parse-errors' = 'true'
);

SELECT auction, price FROM bid WHERE MOD(auction, 123) = 0;