package ru.wobot.flink;

import com.google.gson.GsonBuilder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.wobot.sm.core.api.VkApiTypes;
import ru.wobot.sm.core.mapping.PostProperties;
import ru.wobot.sm.core.mapping.ProfileProperties;
import ru.wobot.sm.core.meta.ContentMetaConstants;
import ru.wobot.sm.core.parse.ParseResult;

import java.util.HashMap;
import java.util.Map;

public class NutchWritableReducer2 implements org.apache.flink.api.common.functions.GroupReduceFunction<org.apache.flink.api.java.tuple.Tuple2<org.apache.hadoop.io.Text, org.apache.nutch.crawl.NutchWritable>, org.apache.flink.api.java.tuple.Tuple3<IndexableType, org.apache.hadoop.io.Text, java.util.Map<String, String>>> {
    public static final Logger LOG = LoggerFactory
            .getLogger(NutchWritableReducer2.class);

    public void reduce(Iterable<Tuple2<Text, NutchWritable>> values, Collector<Tuple3<IndexableType, Text, Map<String, String>>> out) throws Exception {
        Text key = null;
        CrawlDatum fetchDatum = null;
        ParseData parseData = null;
        ParseText parseText = null;
        for (Tuple2<Text, NutchWritable> iter : values) {
            key = iter.f0;
            final Writable value = iter.f1.get(); // unwrap
            if (value instanceof CrawlDatum) {
                final CrawlDatum datum = (CrawlDatum) value;
                if (CrawlDatum.hasFetchStatus(datum) && datum.getStatus() != CrawlDatum.STATUS_FETCH_NOTMODIFIED) {
                    // don't index unmodified (empty) pages
                    fetchDatum = datum;
                } else {
                    LOG.error("Unexpected status: " + datum.getStatus());
                    throw new RuntimeException("Unexpected status: " + datum.getStatus());
                }
            } else if (value instanceof ParseData) {
                parseData = (ParseData) value;
            } else if (value instanceof ParseText) {
                parseText = (ParseText) value;
            }
        }

        // Whether to delete GONE or REDIRECTS
        if (parseData == null) {
            return; // only have inlinks
        }

        if (!parseData.getStatus().isSuccess()
                || fetchDatum.getStatus() != CrawlDatum.STATUS_FETCH_SUCCESS) {
            return;
        }

        final Metadata metadata = parseData.getContentMeta();
        if (metadata.get(ContentMetaConstants.SKIP_FROM_ELASTIC_INDEX) != null
                && metadata.get(ContentMetaConstants.SKIP_FROM_ELASTIC_INDEX).equals("1"))
            return;


        final Map<String, String> hashMap = new HashMap<String, String>() {{
            put(Nutch.SEGMENT_NAME_KEY, metadata.get(Nutch.SEGMENT_NAME_KEY));
            put(Nutch.SIGNATURE_KEY, metadata.get(Nutch.SIGNATURE_KEY));
        }};

        final boolean isSingleDoc = !"true".equals(metadata.get(ContentMetaConstants.MULTIPLE_PARSE_RESULT));

        if (isSingleDoc) {
            out.collect(Tuple3.of(IndexableType.PROFILE, key, hashMap));
        } else {
            if (parseText != null && !StringUtil.isEmpty(parseText.getText())) {
                ParseResult[] parseResults = fromJson(parseText.getText(), ParseResult[].class);
                for (ParseResult parseResult : parseResults) {
                    String subType = (String) parseResult.getContentMeta().get(ContentMetaConstants.TYPE);
                    if (subType == null) {
                        subType = parseData.getContentMeta().get(ContentMetaConstants.TYPE);
                    }
                    if (subType.equals(VkApiTypes.POST)) {
                        final Map<String, Object> parseMeta = parseResult.getParseMeta();
                        final String profileId = (String) parseMeta.get(PostProperties.PROFILE_ID);
                        hashMap.put(PostProperties.PROFILE_ID, profileId);
                        hashMap.put(PostProperties.ID, parseResult.getUrl());

                        out.collect(Tuple3.of(IndexableType.POST, new Text(profileId), hashMap));
                    }
                }
            }
        }

    }


    private static <T> T fromJson(String json, Class<T> classOfT) {
        return new GsonBuilder()
                .create()
                .fromJson(json, classOfT);
    }

}
