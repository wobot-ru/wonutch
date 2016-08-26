#!/usr/bin/env bash

export HADOOP_CONF_DIR=/etc/hadoop/conf
export FLINK_HOME=/opt/flink-1.1-SNAPSHOT
export FLINK_BIN=$FLINK_HOME/bin


if [ -z "$1" ]; then
	echo "Segment not defined."
	echo "Usages: nutch2kafka <segment_path>"
fi

SEGMENT=hdfs://hdp-01/user/nutch/"$1"

if [ -n "$1" ]; then
   $FLINK_BIN/flink run --yarnname FB-To-Kafka --jobmanager yarn-cluster -yn 4 -ys 8 -yjm 5120 -ytm 8096 -yt $FLINK_HOME/lib $FLINK_HOME/lib/etl-0.2.0-SNAPSHOT.jar --nutch --nutch-extract --nutch-publish --bootstrap.servers "hdp-01:6667,hdp-02:6667,hdp-03:6667,hdp-04:6667" --seg $SEGMENT --topic-post FB-POST --topic-profile FB-PROFILE --batch-size 1 --request.timeout.ms 60000
fi


