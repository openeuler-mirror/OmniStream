SET sql-client.execution.result-mode=TABLEAU;
------------------------------------q10------------------------------------------------------------


CREATE TABLE bid (
                     auction  BIGINT,
                     bidder  BIGINT,
                     price  BIGINT,
                     dateTime  TIMESTAMP(3),
                     extra  VARCHAR,
                     dt STRING,
                     hm STRING
) WITH (
      'connector' = 'filesystem',
      'path' = '/repo/codehub/test_tool/csv_files/bid10.csv',
      'format' = 'csv',
      'csv.field-delimiter' = ',',
      'csv.ignore-parse-errors' = 'true'
      );




SELECT auction, bidder, price, dateTime, extra, DATE_FORMAT(dateTime, 'yyyy-MM-dd'), DATE_FORMAT(dateTime, 'HH:mm')
FROM bid;
