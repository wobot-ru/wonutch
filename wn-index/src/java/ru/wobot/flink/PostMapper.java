package ru.wobot.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.ParseData;

public class PostMapper implements org.apache.flink.api.common.functions.FlatMapFunction<org.apache.flink.api.java.tuple.Tuple2<org.apache.hadoop.io.Text, org.apache.hadoop.io.Writable>, org.apache.flink.api.java.tuple.Tuple2<org.apache.hadoop.io.Text, org.apache.nutch.crawl.NutchWritable>> {
    public void flatMap(Tuple2<Text, Writable> value, Collector<Tuple2<Text, NutchWritable>> out) throws Exception {
//        if (value.f1 instanceof ParseData) {
//            ParseData parseData = (ParseData) value.f1;
//            final Metadata contentMeta = parseData.getContentMeta();
//            if (contentMeta.get("nutch.content.api.type") != null
//                    && (contentMeta.get("nutch.content.api.type").equals("post")
//                    || contentMeta.get("nutch.content.api.type").equals("post-bulk")
//                    || contentMeta.get("nutch.content.api.type").equals("comment-bulk"))) {
//                NutchWritable nutchWritable = new NutchWritable(value.f1);
//                out.collect(Tuple2.of(value.f0, nutchWritable));
//            }
//        } else {
//            NutchWritable nutchWritable = new NutchWritable(value.f1);
//            out.collect(Tuple2.of(value.f0, nutchWritable));
//        }
        NutchWritable nutchWritable = new NutchWritable(value.f1);
        out.collect(Tuple2.of(value.f0, nutchWritable));
    }
}
