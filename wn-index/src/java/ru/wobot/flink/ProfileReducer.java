package ru.wobot.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProfileReducer implements org.apache.flink.api.common.functions.GroupReduceFunction<org.apache.flink.api.java.tuple.Tuple2<org.apache.hadoop.io.Text, org.apache.nutch.crawl.NutchWritable>, org.apache.flink.api.java.tuple.Tuple2<org.apache.hadoop.io.Text, org.apache.hadoop.hbase.client.Mutation>> {
    public static final Logger LOG = LoggerFactory
            .getLogger(ProfileReducer.class);

    public void reduce(Iterable<Tuple2<Text, NutchWritable>> values, Collector<Tuple2<Text, Mutation>> out) throws Exception {
        Text key = null;
        CrawlDatum fetchDatum = null;
        ParseData parseData = null;
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

        NutchDocument doc = new NutchDocument();
        doc.add("id", key.toString());
        final Metadata metadata = parseData.getContentMeta();

        if (metadata.get("nutch.content.index.elastic.skip") != null
                && metadata.get("nutch.content.index.elastic.skip").equals("1"))
            return;

        // skip any non profile documents
        if (metadata.get("nutch.content.api.type") != null
                && !metadata.get("nutch.content.api.type").equals("profile"))
            return;

        // add segment, used to map from merged index back to segment files
        doc.add(ProfileProperties.SEGMENT, metadata.get(Nutch.SEGMENT_NAME_KEY));

        // add digest, used by dedup
        doc.add(ProfileProperties.DIGEST, metadata.get(Nutch.SIGNATURE_KEY));

        final Parse parse = new ParseImpl(new ParseText(), parseData);

        if (doc == null) {
            return;
        }

        float boost = 1.0f;
        // apply boost to all indexed fields.
        doc.setWeight(boost);
        // store boost for use by explain and dedup
        doc.add("boost", Float.toString(boost));


        for (String tag : parse.getData().getParseMeta().names()) {
            doc.add(tag, parse.getData().getParseMeta().get(tag));
        }


        Put put = new Put(key.getBytes());
        final String segment = (String) doc.getFieldValue(ProfileProperties.SEGMENT);
        if (!StringUtil.isEmpty(segment))
            put.add(ProfileTableConstants.CF_P, ProfileTableConstants.P_SEGMENT, Bytes.toBytes(segment));
        final String digest = (String) doc.getFieldValue(ProfileProperties.DIGEST);
        if (!StringUtil.isEmpty(digest))
            put.add(ProfileTableConstants.CF_P, ProfileTableConstants.P_DIGEST, Bytes.toBytes(digest));
        final String smProfileId = (String) doc.getFieldValue(ProfileProperties.SM_PROFILE_ID);
        if (!StringUtil.isEmpty(smProfileId))
            put.add(ProfileTableConstants.CF_P, ProfileTableConstants.P_SM_PROFILE_ID, Bytes.toBytes(smProfileId));
        final String name = (String) doc.getFieldValue(ProfileProperties.NAME);
        if (!StringUtil.isEmpty(name))
            put.add(ProfileTableConstants.CF_P, ProfileTableConstants.P_NAME, Bytes.toBytes(name));
        final String source = (String) doc.getFieldValue(ProfileProperties.SOURCE);
        if (!StringUtil.isEmpty(source))
            put.add(ProfileTableConstants.CF_P, ProfileTableConstants.P_SOURCE, Bytes.toBytes(source));
        final String href = (String) doc.getFieldValue(ProfileProperties.HREF);
        if (!StringUtil.isEmpty(href))
            put.add(ProfileTableConstants.CF_P, ProfileTableConstants.P_HREF, Bytes.toBytes(href));
        final String city = (String) doc.getFieldValue(ProfileProperties.CITY);
        if (!StringUtil.isEmpty(city))
            put.add(ProfileTableConstants.CF_P, ProfileTableConstants.P_CITY, Bytes.toBytes(city));
        final String gender = (String) doc.getFieldValue(ProfileProperties.GENDER);
        if (!StringUtil.isEmpty(gender))
            put.add(ProfileTableConstants.CF_P, ProfileTableConstants.P_GENDER, Bytes.toBytes(gender));

        String reachStr = (String) doc.getFieldValue(ProfileProperties.REACH);
        if (!StringUtil.isEmpty(reachStr)) {
            try {
                final int reach = Integer.parseInt(reachStr);
                put.add(ProfileTableConstants.CF_P, ProfileTableConstants.P_REACH, Bytes.toBytes(reach));
            } catch (NumberFormatException ex) {
                LOG.trace("Can't parse reach : " + ex.toString());
            }
        }

        String followerCount = (String) doc.getFieldValue(ProfileProperties.FOLLOWER_COUNT);
        if (!StringUtil.isEmpty(followerCount)) {
            try {
                final int followers = Integer.parseInt(followerCount);
                put.add(ProfileTableConstants.CF_P, ProfileTableConstants.P_FOLLOWER_COUNT, Bytes.toBytes(followers));
            } catch (NumberFormatException ex) {
                LOG.trace("Can't parse followers: " + ex.toString());
            }
        }

        String friendCount = (String) doc.getFieldValue(ProfileProperties.FRIEND_COUNT);
        if (!StringUtil.isEmpty(friendCount)) {
            try {
                final int friends = Integer.parseInt(friendCount);
                put.add(ProfileTableConstants.CF_P, ProfileTableConstants.P_FRIEND_COUNT, Bytes.toBytes(friends));
            } catch (NumberFormatException ex) {
                LOG.trace("Can't parse friends: " + ex.toString());
            }
        }

        Tuple2<Text, Mutation> reuse = new Tuple2<Text, Mutation>();
        reuse.f0 = key;
        reuse.f1 = put;
        out.collect(reuse);
    }
}
