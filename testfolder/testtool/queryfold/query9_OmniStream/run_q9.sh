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
create table  bid(
                     auction BIGINT,
                     bidder  BIGINT,
                     price BIGINT,
                     channel VARCHAR,
                     url  VARCHAR,
                     dateTime TIMESTAMP(3),
                     extra VARCHAR
)WITH(
     'connector' = 'filesystem',
     'path' = '${OMNISTREAM_HOME}/testtool/csv_files/bid9.csv',
     'format' = 'csv',
     'csv.field-delimiter' = ',',
     'csv.ignore-parse-errors' = 'true'

     );

create table auction (
                         id BIGINT,
                         itemName STRING,
                         description STRING,
                         initialBid BIGINT,
                         reserve BIGINT,
                         dateTime TIMESTAMP(3),
                         expires TIMESTAMP(3),
                         seller BIGINT,
                         category BIGINT,
                         extra varchar
)WITH(
     'connector' = 'filesystem',
     'path' = '${OMNISTREAM_HOME}/testtool/csv_files/auction9.csv',
     'format' = 'csv',
     'csv.field-delimiter' = ',',
     'csv.ignore-parse-errors' = 'true'

     );


SELECT
    id, itemName, description, initialBid, reserve, dateTime, expires, seller, category, extra,
    auction, bidder, price, bid_dateTime, bid_extra
FROM (
         SELECT A.*, B.auction, B.bidder, B.price, B.dateTime AS bid_dateTime, B.extra AS bid_extra,
                ROW_NUMBER() OVER (PARTITION BY A.id ORDER BY B.price DESC, B.dateTime ASC) AS rownum
         FROM auction A, bid B
         WHERE A.id = B.auction AND B.dateTime BETWEEN A.dateTime AND A.expires
     )
WHERE rownum <= 1;
EOF
