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

SELECT AuctionBids.auction, AuctionBids.num
FROM (
         SELECT
             auction,
             count(*) AS num,
             window_start AS starttime,
             window_end AS endtime
         FROM TABLE(
                 HOP(TABLE bid, DESCRIPTOR(dateTime), INTERVAL '2' SECOND, INTERVAL '10' SECOND))
         GROUP BY auction, window_start, window_end
     ) AS AuctionBids
         JOIN (
    SELECT
        max(CountBids.num) AS maxn,
        CountBids.starttime,
        CountBids.endtime
    FROM (
             SELECT
                 count(*) AS num,
                 window_start AS starttime,
                 window_end AS endtime
             FROM TABLE(
                     HOP(TABLE bid, DESCRIPTOR(dateTime), INTERVAL '2' SECOND, INTERVAL '10' SECOND))
             GROUP BY auction, window_start, window_end
         ) AS CountBids
    GROUP BY CountBids.starttime, CountBids.endtime
) AS MaxBids
              ON AuctionBids.starttime = MaxBids.starttime AND
                 AuctionBids.endtime = MaxBids.endtime AND
                 AuctionBids.num >= MaxBids.maxn;
EOF
