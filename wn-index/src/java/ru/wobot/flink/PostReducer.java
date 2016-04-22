package ru.wobot.flink;

import com.google.gson.GsonBuilder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.wobot.sm.core.api.VkApiTypes;
import ru.wobot.sm.core.mapping.PostProperties;
import ru.wobot.sm.core.meta.ContentMetaConstants;
import ru.wobot.sm.core.parse.ParseResult;

import java.util.Map;

public class PostReducer implements org.apache.flink.api.common.functions.GroupReduceFunction<org.apache.flink.api.java.tuple.Tuple2<org.apache.hadoop.io.Text, org.apache.nutch.crawl.NutchWritable>, org.apache.flink.api.java.tuple.Tuple2<Text, Post>> {
    public static final Logger LOG = LoggerFactory
            .getLogger(PostReducer.class);

    public void reduce(Iterable<Tuple2<Text, NutchWritable>> values, Collector<Tuple2<Text, Post>> out) throws Exception {
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
                } else if (CrawlDatum.STATUS_LINKED == datum.getStatus()
                        || CrawlDatum.STATUS_SIGNATURE == datum.getStatus()
                        || CrawlDatum.STATUS_PARSE_META == datum.getStatus()) {
                    continue;
                } else {
                    throw new RuntimeException("Unexpected status: " + datum.getStatus());
                }
            } else if (value instanceof ParseData) {
                parseData = (ParseData) value;
            } else if (value instanceof ParseText) {
                parseText = (ParseText) value;
            } else if (LOG.isWarnEnabled() && (!(value instanceof ParseText) || (value instanceof Content) || (value instanceof Inlinks))) {
                LOG.warn("Unrecognized type: " + value.getClass());
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

        // skip any non profile documents
        if (metadata.get(ContentMetaConstants.API_TYPE) != null
                && !(metadata.get(ContentMetaConstants.API_TYPE).equals(VkApiTypes.POST)
                || metadata.get(ContentMetaConstants.API_TYPE).equals(VkApiTypes.POST_BULK)
                || metadata.get(ContentMetaConstants.API_TYPE).equals(VkApiTypes.COMMENT_BULK)))
            return;

        final boolean isSingleDoc = !"true".equals(metadata.get(ContentMetaConstants.MULTIPLE_PARSE_RESULT));
        if (isSingleDoc && metadata.get(ContentMetaConstants.API_TYPE).equals(VkApiTypes.POST)) {
            Post post = new Post();
            post.setId(key.toString());
            post.setSegment(metadata.get(Nutch.SEGMENT_NAME_KEY));
            post.setDigest(metadata.get(Nutch.SIGNATURE_KEY));

            Tuple2<Text, Post> reuse = new Tuple2<Text, Post>();
            reuse.f0 = key;
            reuse.f1 = post;
            out.collect(reuse);
        } else {
            if (parseText != null && !StringUtil.isEmpty(parseText.getText())) {
                ParseResult[] parseResults = fromJson(parseText.getText(), ParseResult[].class);
                for (ParseResult parseResult : parseResults) {
                    String subType = (String) parseResult.getContentMeta().get(ContentMetaConstants.TYPE);
                    if (subType == null) {
                        subType = parseData.getContentMeta().get(ContentMetaConstants.TYPE);
                    }
                    if (subType.equals(VkApiTypes.POST)) {
                        Post post = new Post();
                        post.setId(parseResult.getUrl());
                        post.setSegment(metadata.get(Nutch.SEGMENT_NAME_KEY));

                        final Map<String, Object> parseMeta = parseResult.getParseMeta();
                        post.setProfileId((String) parseMeta.get(PostProperties.PROFILE_ID));
                        Tuple2<Text, Post> reuse = new Tuple2<Text, Post>();
                        reuse.f0 = new Text(post.getProfileId());
                        reuse.f1 = post;
                        out.collect(reuse);
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
