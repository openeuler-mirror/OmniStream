SET sql-client.execution.result-mode=TABLEAU;

CREATE TABLE bid (
                     auction  BIGINT,
                     bidder  BIGINT,
                     price BIGINT,
                     dateTime  TIMESTAMP(3)

) WITH (
      'connector' = 'filesystem',
      'path' = '/repo/codehub/testtool/csv_files/fixed_bid_1000.csv',
      'format' = 'csv'
      );



CREATE TABLE nexmark_q0_output (
                                   auction BIGINT,
                                   bidder BIGINT,
                                   price  DECIMAL(23, 3),
                                   dateTime TIMESTAMP(3)
) WITH (
      'connector' = 'filesystem',
      'path' = '/repo/codehub/testtool/query1_flink/',
      'format' = 'csv',
      'csv.field-delimiter' = ',',
      'csv.ignore-parse-errors' = 'true'

      );

INSERT INTO nexmark_q0_output


SELECT
    auction,
    bidder,
    0.908 * price as price, -- convert dollar to euro
    dateTime
FROM bid;
