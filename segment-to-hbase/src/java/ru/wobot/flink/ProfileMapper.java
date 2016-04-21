package ru.wobot.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.crawl.NutchWritable;

public class ProfileMapper implements org.apache.flink.api.common.functions.FlatMapFunction<org.apache.flink.api.java.tuple.Tuple2<org.apache.hadoop.io.Text, org.apache.hadoop.io.Writable>, org.apache.flink.api.java.tuple.Tuple2<org.apache.hadoop.io.Text, org.apache.nutch.crawl.NutchWritable>> {
    public void flatMap(Tuple2<Text, Writable> value, Collector<Tuple2<Text, NutchWritable>> out) throws Exception {
        NutchWritable nutchWritable = new NutchWritable(value.f1);
        out.collect(Tuple2.of(value.f0, nutchWritable));
    }
}
