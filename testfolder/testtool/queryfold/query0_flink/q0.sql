SET sql-client.execution.result-mode=TABLEAU;
-------------------------------------------------------------q0-----------------------------------------------------
CREATE TABLE bid (
                     auction BIGINT,
                     bidder BIGINT,
                     price BIGINT,
                     dateTime TIMESTAMP(3),
                     extra VARCHAR
) WITH (
      'connector' = 'filesystem',
      'path' = '/repo/codehub/OmniStream/testtool/csv_files/fixed_bid_1000.csv',
      'format' = 'csv',
                       'csv.field-delimiter' = ',',
                       'csv.ignore-parse-errors' = 'true'

                       );


CREATE TABLE nexmark_q0_output (
                                   auction BIGINT,
                                   bidder BIGINT,
                                   price BIGINT,
                                   dateTime TIMESTAMP(3),
                                   extra VARCHAR
) WITH (
      'connector' = 'filesystem',
      'path' = '/repo/codehub/OmniStream/testtool/query0_flink/',
      'format' = 'csv',
                                     'csv.field-delimiter' = ',',
                                     'csv.ignore-parse-errors' = 'true'

                                     );

INSERT INTO nexmark_q0_output
SELECT auction, bidder, price, dateTime,extra FROM bid;
