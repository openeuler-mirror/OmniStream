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
                     auction  BIGINT,
                     bidder  BIGINT,
                     price  BIGINT,
                     dateTime  TIMESTAMP(3),
                     extra  VARCHAR,
                     WATERMARK FOR dateTime AS dateTime - INTERVAL '5' SECOND

) WITH (
      'connector' = 'filesystem',
      'path' = '${OMNISTREAM_HOME}/testtool/csv_files/bid7.csv',
      'format' = 'csv',
      'csv.field-delimiter' = ',',
      'csv.ignore-parse-errors' = 'true'
      );

SELECT B.auction, B.price, B.bidder, B.dateTime, B.extra
from bid B
         JOIN (
    SELECT MAX(price) AS maxprice, window_end as dateTime
    FROM TABLE(
            TUMBLE(TABLE bid, DESCRIPTOR(dateTime), INTERVAL '10' SECOND))
    GROUP BY window_start, window_end
) B1
              ON B.price = B1.maxprice
WHERE B.dateTime BETWEEN B1.dateTime  - INTERVAL '10' SECOND AND B1.dateTime;
EOF
