${FLINK_HOME}/bin/stop-cluster.sh

FILE="/tmp/flink_output.txt"
if [ -f "$FILE" ]; then
    rm "$FILE"
fi
#rm /repo/codehub/flink-1.16.3/log/*
${FLINK_HOME}/bin/start-cluster.sh
${FLINK_HOME}/bin/sql-client.sh <<EOF
SET sql-client.execution.result-mode=TABLEAU;
CREATE TABLE person (
                        id BIGINT,
                        name STRING,
                        dateTime TIMESTAMP(3) NOT NULL,
                        WATERMARK FOR dateTime AS dateTime - INTERVAL '10' SECOND
) WITH (
      'connector' = 'filesystem',
      'path' = '${OMNISTREAM_HOME}/testtool/csv_files/person91.csv',
      'format' = 'csv',
      'csv.field-delimiter' = ',',
      'csv.ignore-parse-errors' = 'true'
      );

CREATE TABLE auction (
                         seller BIGINT,
                         dateTime TIMESTAMP(3) NOT NULL,
                         WATERMARK FOR dateTime AS dateTime - INTERVAL '10' SECOND
) WITH (
      'connector' = 'filesystem',
      'path' = '${OMNISTREAM_HOME}/testtool/csv_files/auctionQ91.csv',
      'format' = 'csv',
      'csv.field-delimiter' = ',',
      'csv.ignore-parse-errors' = 'true'
      );

SELECT P.id, P.name, P.starttime
FROM (
         SELECT id, name,
                window_start AS starttime,
                window_end AS endtime
         FROM TABLE(
                 TUMBLE(TABLE person, DESCRIPTOR(dateTime), INTERVAL '10' SECOND))
         GROUP BY id, name, window_start, window_end
     ) P
         JOIN (
    SELECT seller,
           window_start AS starttime,
           window_end AS endtime
    FROM TABLE(
            TUMBLE(TABLE auction, DESCRIPTOR(dateTime), INTERVAL '10' SECOND))
    GROUP BY seller, window_start, window_end
) A
              ON P.id = A.seller AND P.starttime = A.starttime AND P.endtime = A.endtime;
EOF
