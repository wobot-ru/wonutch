package ru.wobot.index.flink;

import com.google.gson.GsonBuilder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
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
import ru.wobot.index.Post;
import ru.wobot.index.Profile;
import ru.wobot.index.Types;
import ru.wobot.sm.core.mapping.PostProperties;
import ru.wobot.sm.core.mapping.ProfileProperties;
import ru.wobot.sm.core.meta.ContentMetaConstants;
import ru.wobot.sm.core.parse.ParseResult;

import java.util.Map;

public class NutchWritableReducer implements org.apache.flink.api.common.functions.GroupReduceFunction<org.apache.flink.api.java.tuple.Tuple2<org.apache.hadoop.io.Text, org.apache.nutch.crawl.NutchWritable>, Tuple4<Types, Text, Post, Profile>> {
    private static final Profile NULL_PROFILE = (Profile) null;
    private static final Post NULL_POST = (Post) null;
    private static final String DIGEST = "digest";
    private static final String CRAWL_DATE = "crawl_date";

    public static final Logger LOG = LoggerFactory
            .getLogger(NutchWritableReducer.class);

    public void reduce(Iterable<Tuple2<Text, NutchWritable>> values, Collector<Tuple4<Types, Text, Post, Profile>> out) throws Exception {
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

        final Metadata contentMeta = parseData.getContentMeta();
        if (contentMeta.get(ContentMetaConstants.SKIP_FROM_ELASTIC_INDEX) != null
                && contentMeta.get(ContentMetaConstants.SKIP_FROM_ELASTIC_INDEX).equals("1"))
            return;


        final String segment = contentMeta.get(Nutch.SEGMENT_NAME_KEY);
        final String score = contentMeta.get(Nutch.SCORE_KEY);
        final String digest = contentMeta.get(Nutch.SIGNATURE_KEY);
        final String crawlDate = contentMeta.get(ContentMetaConstants.FETCH_TIME);
        final boolean isSingleDoc = !"true".equals(contentMeta.get(ContentMetaConstants.MULTIPLE_PARSE_RESULT));

        if (isSingleDoc) {
            final Metadata parseMeta = parseData.getParseMeta();
            if (contentMeta.get(ContentMetaConstants.TYPE).equals(ru.wobot.sm.core.mapping.Types.PROFILE)) {
                final Profile profile = new Profile();
                profile.id = key.toString();
                profile.segment = segment;
                profile.crawlDate = crawlDate;
                profile.score = score;
                profile.digest = digest;
                profile.source = parseMeta.get(ProfileProperties.SOURCE);
                profile.name = parseMeta.get(ProfileProperties.NAME);
                profile.href = parseMeta.get(ProfileProperties.HREF);
                profile.smProfileId = parseMeta.get(ProfileProperties.SM_PROFILE_ID);
                profile.city = parseMeta.get(ProfileProperties.CITY);
                profile.gender = parseMeta.get(ProfileProperties.GENDER);
                final String reach = parseMeta.get(ProfileProperties.REACH);
                if (!StringUtil.isEmpty(reach))
                    profile.reach = Long.parseLong(reach);

                out.collect(Tuple4.of(Types.PROFILE, key, NULL_POST, profile));
            }
        } else {
            if (parseText != null && !StringUtil.isEmpty(parseText.getText())) {
                ParseResult[] parseResults = fromJson(parseText.getText(), ParseResult[].class);
                for (ParseResult parseResult : parseResults) {
                    String subType = (String) parseResult.getContentMeta().get(ContentMetaConstants.TYPE);
                    if (subType == null) {
                        subType = parseData.getContentMeta().get(ContentMetaConstants.TYPE);
                    }
                    if (subType.equals(ru.wobot.sm.core.mapping.Types.POST)) {
                        final Map<String, Object> parseMeta = parseResult.getParseMeta();
                        final long smPostId = Math.round((Double) parseMeta.get(PostProperties.SM_POST_ID));
                        final long engagement = Math.round((Double) parseMeta.get(PostProperties.ENGAGEMENT));

                        final Post post = new Post();
                        post.id = parseResult.getUrl();
                        post.crawlDate = crawlDate;
                        post.digest = (String) parseResult.getContentMeta().get(DIGEST);
                        post.segment = segment;
                        post.score = score;
                        post.source = (String) parseMeta.get(PostProperties.SOURCE);
                        post.profileId = (String) parseMeta.get(PostProperties.PROFILE_ID);
                        post.href = (String) parseMeta.get(PostProperties.HREF);
                        post.smPostId = smPostId;
                        post.body = (String) parseMeta.get(PostProperties.BODY);
                        post.date = (String) parseMeta.get(PostProperties.POST_DATE);
                        post.isComment = (Boolean) parseMeta.get(PostProperties.IS_COMMENT);
                        post.engagement = engagement;
                        post.parentPostId = (String) parseMeta.get(PostProperties.PARENT_POST_ID);
                        out.collect(Tuple4.of(Types.POST, new Text(post.profileId), post, NULL_PROFILE));
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
