${FLINK_HOME}/bin/stop-cluster.sh


FILE="/tmp/flink_output.txt"
if [ -f "$FILE" ]; then
    rm "$FILE"
fi
#rm /repo/codehub/flink-1.16.3/log/*

${FLINK_HOME}/bin/start-cluster.sh
${FLINK_HOME}/bin/sql-client.sh <<EOF
SET sql-client.execution.result-mode=TABLEAU;
------------------------------------q21------------------------------------------------------------
CREATE TABLE bid (
                     auction BIGINT,
                     bidder BIGINT,
                     price BIGINT,
                     channel VARCHAR,
                     url VARCHAR,
                     dateTime TIMESTAMP(3),
                     extra VARCHAR
) WITH (
      'connector' = 'filesystem',
      'path' = '${OMNISTREAM_HOME}/testtool/csv_files/bid20.csv',
      'format' = 'csv',
      'csv.field-delimiter' = ',',
      'csv.ignore-parse-errors' = 'true'
      );
CREATE TABLE auction (
                         id BIGINT,
                         itemName VARCHAR,
                         description VARCHAR,
                         initialBid BIGINT,
                         reserve BIGINT,
                         dateTime TIMESTAMP(3),
                         expires TIMESTAMP(3),
                         seller BIGINT,
                         category BIGINT,
                         extra VARCHAR
) WITH (
      'connector' = 'filesystem',
      'path' = '${OMNISTREAM_HOME}/testtool/csv_files/auction20.csv',
      'format' = 'csv',
      'csv.field-delimiter' = ',',
      'csv.ignore-parse-errors' = 'true'
      );
SELECT
    auction, bidder, price, channel, url, B.dateTime, B.extra,
    itemName, description, initialBid, reserve, A.dateTime, expires, seller, category, A.extra
FROM
    bid AS B INNER JOIN auction AS A on B.auction = A.id;
EOF

