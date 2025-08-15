${FLINK_HOME}/bin/stop-cluster.sh

FILE="/tmp/flink_output.txt"
if [ -f "$FILE" ]; then
    rm "$FILE"
fi
#rm /repo/codehub/flink-1.16.3/log/*
echo
${FLINK_HOME}/bin/start-cluster.sh
${FLINK_HOME}/bin/sql-client.sh <<EOF
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
      'path' = '${OMNISTREAM_HOME}/testtool/csv_files/person.csv',
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
                         category BIGINT
) WITH (
      'connector' = 'filesystem',
      'path' = '${OMNISTREAM_HOME}/testtool/csv_files/auction.csv',
      'format' = 'csv',
      'csv.field-delimiter' = ',',
      'csv.ignore-parse-errors' = 'true'
      );



SELECT
    P.name, P.city, P.state, A.id
FROM
    auction AS A INNER JOIN person AS P on A.seller = P.id
WHERE
    A.category = 10 and (P.state = 'OR' OR P.state = 'ID' OR P.state = 'CA');


EOF
