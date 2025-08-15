/repo/codehub/flink-1.16.3/bin/stop-cluster.sh

rm /repo/codehub/flink-1.16.3/log/*

/repo/codehub/flink-1.16.3/bin/start-cluster.sh
/repo/codehub/flink-1.16.3/bin/sql-client.sh -f $1
