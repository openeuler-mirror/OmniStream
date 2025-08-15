${FLINK_HOME}/bin/stop-cluster.sh

FILE="/tmp/flink_output.txt"
if [ -f "$FILE" ]; then
    rm "$FILE"
fi
#rm /repo/codehub/flink-1.16.3/log/*
${FLINK_HOME}/bin/start-cluster.sh


CSV_PATH="${OMNISTREAM_HOME}/testtool/csv_files/bid16.csv"
sed "s|\${CSV_PATH}|${CSV_PATH}|g" ${OMNISTREAM_HOME}/testtool/queryfold/query16_OmniStream/query16.sql > ${OMNISTREAM_HOME}/testtool/queryfold/query16_OmniStream/query16_filled.sql
${FLINK_HOME}/bin/sql-client.sh -f $1
rm -r ${OMNISTREAM_HOME}/testtool/queryfold/query16_OmniStream/query16_filled.sql


