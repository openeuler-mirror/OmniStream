SET sql-client.execution.result-mode=TABLEAU;
------------------------------------q7------------------------------------------------------------

create table  bid(
                     auction BIGINT,
                     bidder  BIGINT,
                     price BIGINT,
                     channel VARCHAR,
                     url  VARCHAR,
                     dateTime timestamp(3),
                     extra VARCHAR
)WITH(
     'connector' = 'filesystem',
     'path' = '/repo/codehub/test_tool/bid9.csv',
     'format' = 'csv',
     'csv.field-delimiter' = ',',
     'csv.ignore-parse-errors' = 'true'

     );

create table auction (
                         id BIGINT,
                         itemName STRING,
                         description STRING,
                         initialBid BIGINT,
                         reserve BIGINT,
                         dateTime timestamp(3),
                         expires timestamp(3),
                         seller BIGINT,
                         category INT,
                         extra varchar
)WITH(
     'connector' = 'filesystem',
     'path' = '/repo/codehub/test_tool/auction9.csv',
     'format' = 'csv',
     'csv.field-delimiter' = ',',
     'csv.ignore-parse-errors' = 'true'

     );

SELECT
    Q.category,
    AVG(Q.final)
FROM (
         SELECT MAX(B.price) AS final, A.category
         FROM auction A, bid B
         WHERE A.id = B.auction AND B.dateTime BETWEEN A.dateTime AND A.expires
         GROUP BY A.id, A.category
     ) Q
GROUP BY Q.category;
