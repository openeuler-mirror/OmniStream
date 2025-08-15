${FLINK_HOME}/bin/stop-cluster.sh

FILE="/tmp/flink_output.txt"
if [ -f "$FILE" ]; then
    rm "$FILE"
fi

envsubst < query15.sql > temp_query.sql

#rm /repo/codehub/flink-1.16.3/log/*
${FLINK_HOME}/bin/start-cluster.sh

CSV_PATH="${OMNISTREAM_HOME}/testtool/csv_files/bids_data.csv"
sed "s|\${CSV_PATH}|${CSV_PATH}|g" ${OMNISTREAM_HOME}/testtool/queryfold/query15_OmniStream/query15.sql > ${OMNISTREAM_HOME}/testtool/queryfold/query15_OmniStream/query15_filled.sql
${FLINK_HOME}/bin/sql-client.sh -f $1
rm -r ${OMNISTREAM_HOME}/testtool/queryfold/query15_OmniStream/query15_filled.sql
