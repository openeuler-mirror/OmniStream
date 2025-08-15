#/repo/codehub/flink-1.16.3/bin/stop-cluster.sh

rm /repo/codehub/test_tool//query1_flink/q1.txt
rm /repo/codehub/flink-1.16.3/log/*

#/repo/codehub/flink-1.16.3/bin/start-cluster.sh
/repo/codehub/flink-1.16.3/bin/sql-client.sh <<EOF
SET sql-client.execution.result-mode=TABLEAU;
-------------------------------------------------------------q2-----------------------------------------------------
CREATE TABLE person (
                        id BIGINT,
                        name STRING,
                        email STRING,
                        creditCard STRING,
                        city STRING,
                        state STRING
) WITH (
      'connector' = 'filesystem',
      'path' = '/repo/codehub/test_tool/csv_files/person.csv',
      'format' = 'csv',
      'csv.field-delimiter' = ',',
      'csv.ignore-parse-errors' = 'true'
      );

CREATE TABLE auction (
                         id BIGINT,
                         itemName STRING,
                         description STRING,
                         initialBid BIGINT,
                         reserve BIGINT,
--                          dateTime TIMESTAMP(3),
                         expires TIMESTAMP(3),
                         seller BIGINT,
                         category INT
) WITH (
      'connector' = 'filesystem',
      'path' = '/repo/codehub/test_tool/csv_files/auction.csv',
      'format' = 'csv',
      'csv.field-delimiter' = ',',
      'csv.ignore-parse-errors' = 'true'
      );



-- CREATE TABLE nexmark_q0_output (
--                                    name  VARCHAR,
--                                    city  VARCHAR,
--                                    state  VARCHAR,
--                                    id  BIGINT
-- ) WITH (
--       'connector' = 'filesystem',
--       'path' = '/repo/codehub/test_tool/test_flink/',
--       'format' = 'csv',
--       'csv.field-delimiter' = ',',
--       'csv.ignore-parse-errors' = 'true'
--
--       );
--
--
-- INSERT INTO nexmark_q0_output
SELECT
    P.name, P.city, P.state, A.id
FROM
    auction AS A INNER JOIN person AS P on A.seller = P.id
WHERE
    A.category = 10 and (P.state = 'OR' OR P.state = 'ID' OR P.state = 'CA');

EOF

