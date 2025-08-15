${FLINK_HOME}/bin/stop-cluster.sh

FILE="/tmp/flink_output.txt"
if [ -f "$FILE" ]; then
    rm "$FILE"
fi
#rm /repo/codehub/flink-1.16.3/log/*
${FLINK_HOME}/bin/start-cluster.sh
CSV_PATH="${OMNISTREAM_HOME}/testtool/csv_files/bid20_test_copy.csv"
sed "s|\${CSV_PATH}|${CSV_PATH}|g" ${OMNISTREAM_HOME}/testtool/queryfold/query17_OmniStream/query17.sql > ${OMNISTREAM_HOME}/testtool/queryfold/query17_OmniStream/query17_filled.sql
${FLINK_HOME}/bin/sql-client.sh -f $1
rm -r ${OMNISTREAM_HOME}/testtool/queryfold/query17_OmniStream/query17_filled.sql

