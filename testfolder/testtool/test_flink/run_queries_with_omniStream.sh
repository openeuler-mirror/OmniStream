/repo/codehub/flink-1.16.3/bin/stop-cluster.sh
/repo/codehub/flink-1.16.3/bin/stop-cluster.sh

rm /tmp/flink_output.txt
rm /repo/codehub/flink-1.16.3/log/*

/repo/codehub/flink-1.16.3/bin/start-cluster.sh

${FLINK_HOME}/bin/sql-client.sh -f $1