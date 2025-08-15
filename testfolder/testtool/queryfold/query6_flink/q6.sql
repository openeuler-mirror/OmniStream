CREATE TABLE auction (
                         id BIGINT,
                         seller STRING,
                         dateTime TIMESTAMP(3),
                         expires TIMESTAMP(3),
                         WATERMARK FOR dateTime AS dateTime - INTERVAL '5' SECOND
) WITH (
      'connector' = 'filesystem',
      'path' = '/repo/codehub/OmniStream/testtool/csv_files/auction6.csv',
      'format' = 'csv',
      'csv.ignore-parse-errors' = 'true',
      'csv.field-delimiter' = ','
      );
CREATE TABLE bid (
                     auction BIGINT,
                     price BIGINT,
                     dateTime TIMESTAMP(3),
                     WATERMARK FOR dateTime AS dateTime - INTERVAL '5' SECOND
) WITH (
      'connector' = 'filesystem',
      'path' = '/repo/codehub/OmniStream/testtool/csv_files/bid6.csv',
      'format' = 'csv',
      'csv.ignore-parse-errors' = 'true',
      'csv.field-delimiter' = ','
      );

SELECT
    Q.seller,
    AVG(Q.price) OVER
    (PARTITION BY Q.seller ORDER BY Q.dateTime ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)
FROM (
         SELECT *, ROW_NUMBER() OVER (PARTITION BY A.id, A.seller ORDER BY B.price DESC) AS rownum
         FROM (SELECT A.id, A.seller, B.price, B.dateTime
               FROM auction AS A,
                    bid AS B
               WHERE A.id = B.auction
                 and B.dateTime between A.dateTime and A.expires)
         WHERE rownum <= 1
     ) AS Q;