SET sql-client.execution.result-mode=TABLEAU;


-------------------------------------------------------------q3-----------------------------------------------------
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