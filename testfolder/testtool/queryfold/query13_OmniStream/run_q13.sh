${FLINK_HOME}/bin/stop-cluster.sh

FILE="/tmp/flink_output.txt"
[ -f "$FILE" ] && rm "$FILE"

${FLINK_HOME}/bin/start-cluster.sh

BID_PATH="${OMNISTREAM_HOME}/testtool/csv_files/side_input.csv"
BID_PATH2="${OMNISTREAM_HOME}/testtool/csv_files/bids_data.csv"

cat > temp_query.sql <<EOF
SET sql-client.execution.result-mode=TABLEAU;

CREATE FUNCTION count_char AS 'com.github.nexmark.flink.udf.CountChar';

CREATE TABLE side_input (
    key BIGINT,
    \`value\` VARCHAR
) WITH (
    'connector.type' = 'filesystem',
    'connector.path' = '${BID_PATH}',
    'format.type' = 'csv'
);

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
    'path' = '${BID_PATH2}',
    'format' = 'csv',
    'csv.field-delimiter' = ',',
    'csv.ignore-parse-errors' = 'true'
);

SELECT
    B.auction,
    B.bidder,
    B.price,
    B.dateTime,
    S.\`value\`
FROM (
    SELECT *, PROCTIME() AS p_time
    FROM bid
) AS B
JOIN side_input FOR SYSTEM_TIME AS OF B.p_time AS S
    ON MOD(B.auction, 10000) = S.key;
EOF

${FLINK_HOME}/bin/sql-client.sh -f temp_query.sql
rm -r temp_query.sql

