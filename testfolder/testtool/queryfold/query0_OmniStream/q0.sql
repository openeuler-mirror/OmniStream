SET sql-client.execution.result-mode=TABLEAU;
--        q0
CREATE TABLE bid (
                     auction BIGINT,
                     bidder BIGINT,
                     price BIGINT,
                     dateTime TIMESTAMP(3)
) WITH (
      'connector' = 'filesystem',
      'path' = '/repo/codehub/testtool/csv_files/fixed_bid_1000.csv',
      'format' = 'csv'
      );


SELECT auction, bidder, price, dateTime FROM bid;
--
--
