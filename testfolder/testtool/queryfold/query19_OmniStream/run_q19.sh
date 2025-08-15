${FLINK_HOME}/bin/stop-cluster.sh


FILE="/tmp/flink_output.txt"
if [ -f "$FILE" ]; then
    rm "$FILE"
fi
#rm /repo/codehub/flink-1.16.3/log/*

${FLINK_HOME}/bin/start-cluster.sh
/repo/codehub/flink-1.16.3/bin/sql-client.sh <<EOF
SET sql-client.execution.result-mode=TABLEAU;
create table  bid(
                     auction BIGINT,
                     bidder  BIGINT,
                     price BIGINT,
                     channel VARCHAR,
                     url  VARCHAR,
                     datetime TIMESTAMP(3),
                     extra VARCHAR
)WITH(
     'connector' = 'filesystem',
     'path' = '${OMNISTREAM_HOME}/testtool/csv_files/bids_data.csv',
     'format' = 'csv',
     'csv.field-delimiter' = ',',
     'csv.ignore-parse-errors' = 'true'

     )


-- CREATE TABLE nexmark_q19 (
--                              auction  BIGINT,
--                              bidder  BIGINT,
--                              price  BIGINT,
--                              channel  VARCHAR,
--                              url  VARCHAR,
--                              dateTime  TIMESTAMP(3),
--                              extra  VARCHAR,
--                              rank_number  BIGINT
-- ) WITH (
--       'connector' = 'blackhole'
--       );
-- INSERT INTO nexmark_q19

SELECT * FROM
    (SELECT *, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rank_number FROM bid)
WHERE rank_number <= 10;
EOF



