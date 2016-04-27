package ru.wobot.index.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.protocol.Content;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NutchWritableMapper implements org.apache.flink.api.common.functions.FlatMapFunction<org.apache.flink.api.java.tuple.Tuple2<org.apache.hadoop.io.Text, org.apache.hadoop.io.Writable>, org.apache.flink.api.java.tuple.Tuple2<org.apache.hadoop.io.Text, org.apache.nutch.crawl.NutchWritable>> {
    public static final Logger LOG = LoggerFactory
            .getLogger(NutchWritableMapper.class);

    public void flatMap(Tuple2<Text, Writable> value, Collector<Tuple2<Text, NutchWritable>> out) throws Exception {
        if (value.f1 instanceof CrawlDatum) {
            CrawlDatum datum = (CrawlDatum) value.f1;
            if (!(CrawlDatum.STATUS_LINKED == datum.getStatus()
                    || CrawlDatum.STATUS_SIGNATURE == datum.getStatus()
                    || CrawlDatum.STATUS_PARSE_META == datum.getStatus()))
                out.collect(Tuple2.of(value.f0, new NutchWritable(value.f1)));
            return;
        }
        if (value.f1 instanceof Content) return;
        if (value.f1 instanceof Inlinks) return;


        out.collect(Tuple2.of(value.f0, new NutchWritable(value.f1)));
        //if (LOG.isWarnEnabled() && (!(value.f1 instanceof ParseData || value.f1 instanceof ParseText))) {
        //LOG.warn("Unrecognized type: " + value.getClass());
        //}
    }
}
