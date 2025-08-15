${FLINK_HOME}/bin/stop-cluster.sh

FILE="/tmp/flink_output.txt"
if [ -f "$FILE" ]; then
    rm "$FILE"
fi
#rm /repo/codehub/flink-1.16.3/log/*
${FLINK_HOME}/bin/start-cluster.sh
${FLINK_HOME}/bin/sql-client.sh <<EOF
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
      'path' = '${OMNISTREAM_HOME}/testtool/csv_files/bid10.csv',
      'format' = 'csv',
      'csv.field-delimiter' = ',',
      'csv.ignore-parse-errors' = 'true'
      );




SELECT auction, bidder, price, dateTime, extra, DATE_FORMAT(dateTime, 'yyyy-MM-dd'), DATE_FORMAT(dateTime, 'HH:mm')
FROM bid;
EOF
