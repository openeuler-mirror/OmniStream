/repo/codehub/flink-1.16.3/bin/stop-cluster.sh

rm /repo/codehub/flink-1.16.3/log/*

/repo/codehub/flink-1.16.3/bin/start-cluster.sh
/repo/codehub/flink-1.16.3/bin/sql-client.sh <<EOF
SET sql-client.execution.result-mode=TABLEAU;
CREATE TABLE bid (
                     auction BIGINT,
                     bidder BIGINT,
                     price BIGINT,
                     dateTime TIMESTAMP(3),
                     extra VARCHAR
) WITH (
      'connector' = 'filesystem',
      'path' = '/repo/codehub/test_tool/csv_files/fixed_bid_1000.csv',
      'format' = 'csv',
                       'csv.field-delimiter' = ',',
                       'csv.ignore-parse-errors' = 'true'

                       );


CREATE TABLE nexmark_q0_output (
                                   auction BIGINT,
                                   bidder BIGINT,
                                   price BIGINT,
                                   dateTime TIMESTAMP(3),
                                   extra VARCHAR
) WITH (
      'connector' = 'filesystem',
      'path' = '/repo/codehub/test_tool/query0_flink/',
      'format' = 'csv',
                                     'csv.field-delimiter' = ',',
                                     'csv.ignore-parse-errors' = 'true'

                                     );

INSERT INTO nexmark_q0_output
SELECT auction, bidder, price, dateTime,extra FROM bid;
EOF






