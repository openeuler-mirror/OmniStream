${FLINK_HOME}/bin/stop-cluster.sh

FILE="/tmp/flink_output.txt"
if [ -f "$FILE" ]; then
    rm "$FILE"
fi
#rm /repo/codehub/flink-1.16.3/log/*
${FLINK_HOME}/bin/start-cluster.sh
${FLINK_HOME}/bin/sql-client.sh <<EOF
SET sql-client.execution.result-mode=TABLEAU;

CREATE TABLE bid (
                     bidder BIGINT,
                     bid_count BIGINT,
                     dateTime TIMESTAMP(3),
                     WATERMARK FOR dateTime AS dateTime - INTERVAL '2' SECOND

) WITH (
      'connector' = 'filesystem',
      'path' = '${OMNISTREAM_HOME}/testtool/csv_files/bid11.csv',
      'format' = 'csv',
      'csv.field-delimiter' = ',',
      'csv.ignore-parse-errors' = 'true'
      );

SELECT
    B.bidder,
    count(*) as bid_count,
    SESSION_START(B.dateTime, INTERVAL '10' SECOND) as starttime,
    SESSION_END(B.dateTime, INTERVAL '10' SECOND) as endtime
FROM bid B
GROUP BY B.bidder, SESSION(B.dateTime, INTERVAL '10' SECOND);
EOF
