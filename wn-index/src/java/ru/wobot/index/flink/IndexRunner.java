package ru.wobot.index.flink;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import com.twitter.chill.java.UUIDSerializer;
import org.apache.avro.Schema;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormat;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.api.java.io.TupleCsvInputFormat;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.runtime.AvroSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.Serializers;
import org.apache.flink.shaded.com.google.common.reflect.TypeToken;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.*;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch2.RequestIndexer;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.segment.SegmentChecker;
import org.apache.nutch.util.HadoopFSUtil;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.wobot.index.*;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexRunner {
    public static final Logger LOG = LoggerFactory
            .getLogger(IndexRunner.class);

    public static void main(String[] args) throws Exception {

        final IndexParams.Params params = IndexParams.parse(args);
        System.out.println(params);
        if (!params.canExecute()) return;

        long startTime = System.currentTimeMillis();
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final Job profileMapJob = Job.getInstance();

        final String[] segs = params.getSegs();
        if (segs != null)
            addSegments(profileMapJob, segs);
        final String[] dirs = params.getDirs();
        if (dirs != null)
            addSegmentDirs(profileMapJob, dirs);

        DataSource<Tuple2<Text, Writable>> input = env.createInput(new HadoopInputFormat<Text, Writable>(new SequenceFileInputFormat<Text, Writable>(), Text.class, Writable.class, profileMapJob));
        FlatMapOperator<Tuple2<Text, Writable>, Tuple2<Text, NutchWritable>> flatMap = input.flatMap(new NutchWritableMapper());


        final GroupReduceOperator<Tuple2<Text, NutchWritable>, Tuple4<Types, Text, Post, Profile>> reduceGroup = flatMap.groupBy(0).reduceGroup(new NutchWritableReducer());
        final ProjectOperator<?, Tuple2<Text, Post>> postProj = reduceGroup.project(1, 2);
        final FilterOperator<Tuple2<Text, Post>> posts = postProj.filter(new FilterFunction<Tuple2<Text, Post>>() {
            public boolean filter(Tuple2<Text, Post> tuple) throws Exception {
                return tuple.f1 != null;
            }
        });
        final ProjectOperator<?, Tuple2<Text, Profile>> profileProj = reduceGroup.project(1, 3);
        final FilterOperator<Tuple2<Text, Profile>> profiles = profileProj.filter(new FilterFunction<Tuple2<Text, Profile>>() {
            public boolean filter(Tuple2<Text, Profile> tuple) throws Exception {
                return tuple.f1 != null;
            }
        });

        FlatJoinFunction<Tuple2<Text, Post>, Tuple2<Text, Profile>, PostTuple> join = new FlatJoinFunction<Tuple2<Text, Post>, Tuple2<Text, Profile>, PostTuple>() {
            public void join(Tuple2<Text, Post> tp, Tuple2<Text, Profile> ta, Collector<PostTuple> collector) throws Exception {
                final Post post = tp.f1;
                final Profile profile = ta.f1;
                final PostDetails result = new PostDetails();
                result.id = post.id;
                if (post.crawlDate != null)
                    result.crawlDate = post.crawlDate;
                if (post.digest != null)
                    result.digest = post.digest;
                if (post.score != null)
                    result.score = post.score;
                result.segment = post.segment + "-" + profile.segment;
                if (post.source != null)
                    result.source = post.source;
                result.isComment = post.isComment;
                result.engagement = post.engagement;
                if (post.parentPostId != null)
                    result.parentPostId = post.parentPostId;
                if (post.body != null)
                    result.body = post.body;
                if (post.date != null)
                    result.date = post.date;
                if (post.href != null)
                    result.href = post.href;
                if (post.smPostId != null)
                    result.smPostId = post.smPostId;
                if (profile.city != null)
                    result.city = profile.city;
                if (profile.gender != null)
                    result.gender = profile.gender;
                if (profile.href != null)
                    result.profileHref = profile.href;
                if (profile.id != null)
                    result.profileId = profile.id;
                if (profile.name != null)
                    result.profileName = profile.name;
                result.reach = profile.reach;
                if (profile.smProfileId != null)
                    result.smProfileId = profile.smProfileId;

                collector.collect(new PostTuple(
                        result.id,
                        result.body,
                        result.city,
                        result.crawlDate,
                        result.date,
                        result.digest,
                        result.engagement,
                        result.gender,
                        result.href,
                        result.isComment,
                        result.parentPostId,
                        result.profileHref,
                        result.profileId,
                        result.profileName,
                        result.reach,
                        result.score,
                        result.segment,
                        result.smPostId,
                        result.smProfileId,
                        result.source
                ));
            }
        };


        DataSet<PostTuple> denorm = posts.join(profiles).where(0).equalTo(0).with(join);
        //env.getConfig().enableForceAvro();
        //env.getConfig().enableForceKryo();

        final String flinkTmp = params.getFlinkTmpDir() + System.currentTimeMillis();
        final DataSink<PostTuple> tuple1DataSink = denorm.writeAsCsv(flinkTmp, CsvInputFormat.DEFAULT_LINE_DELIMITER, CsvInputFormat.DEFAULT_FIELD_DELIMITER, org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE);

        //final long totalPosts = -1;
        final long totalPosts = denorm.count();
        LOG.info("Total posts to import:" + totalPosts);
        //env.execute("save to tmp");


        final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();


        TupleTypeInfo<Tuple20<String, String, String, String, String, String, Long, String, String, Boolean, String, String, String, String, Long, String, String, String, String, String>>
                typeInfo = new TupleTypeInfo<Tuple20<String, String, String, String, String, String, Long, String, String, Boolean, String, String, String, String, Long, String, String, String, String, String>>(
                BasicTypeInfo.STRING_TYPE_INFO,   //String id,          0
                BasicTypeInfo.STRING_TYPE_INFO,   //String body,        1
                BasicTypeInfo.STRING_TYPE_INFO,   //String city,        2
                BasicTypeInfo.STRING_TYPE_INFO,   //String crawlDate,   3
                BasicTypeInfo.STRING_TYPE_INFO,   //String date,        4
                BasicTypeInfo.STRING_TYPE_INFO,   //String digest,      5
                BasicTypeInfo.LONG_TYPE_INFO,     //long engagement,    6
                BasicTypeInfo.STRING_TYPE_INFO,   //String gender,      7
                BasicTypeInfo.STRING_TYPE_INFO,   //String href,        8
                BasicTypeInfo.BOOLEAN_TYPE_INFO,  //Boolean isComment,  9
                BasicTypeInfo.STRING_TYPE_INFO,   //String parentPostId 10
                BasicTypeInfo.STRING_TYPE_INFO,   //String profileHref, 11
                BasicTypeInfo.STRING_TYPE_INFO,   //String profileId,   12
                BasicTypeInfo.STRING_TYPE_INFO,   //String profileName, 13
                BasicTypeInfo.LONG_TYPE_INFO,     //long reach,         14
                BasicTypeInfo.STRING_TYPE_INFO,   //String score,       15
                BasicTypeInfo.STRING_TYPE_INFO,   //String segment,     16
                BasicTypeInfo.STRING_TYPE_INFO,   //String smPostId,    17
                BasicTypeInfo.STRING_TYPE_INFO,   //String smProfileId, 18
                BasicTypeInfo.STRING_TYPE_INFO);  //String source       19

        final TupleCsvInputFormat<Tuple20<String, String, String, String, String, String, Long, String, String, Boolean, String, String, String, String, Long, String, String, String, String, String>>
                inputFormat = new TupleCsvInputFormat<Tuple20<String, String, String, String, String, String, Long, String, String, Boolean, String, String, String, String, Long, String, String, String, String, String>>(new org.apache.flink.core.fs.Path(flinkTmp), typeInfo);
        inputFormat.setLenient(true);
        final DataStreamSource<Tuple20<String, String, String, String, String, String, Long, String, String, Boolean, String, String, String, String, Long, String, String, String, String, String>> source = streamEnv.createInput(inputFormat, typeInfo);

//        final WindowedStream<Tuple20<String, String, String, String, String, String, Long, String, String, Boolean, String, String, String, String, Long, String, String, String, String, String>, Tuple, TimeWindow> timeWindow = source.keyBy(0).timeWindow(Time.seconds(120), Time.seconds(1));
//        final SingleOutputStreamOperator<Tuple20<String, String, String, String, String, String, Long, String, String, Boolean, String, String, String, String, Long, String, String, String, String, String>> tumblingwindow = timeWindow.maxBy(6).name("timeWindow");
        //tumblingwindow.addSink(new PrintSinkFunction<PostTuple>(true));

        Map<String, String> config = new HashMap<String, String>();
        config.put(ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, params.getMaxActions());
        config.put(ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB, "5");
        //config.put(ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_INTERVAL_MS, "30000");
        config.put("cluster.name", params.getEsCluster());
        config.put("client.transport.ignore_cluster_name", "true");

        List<InetSocketAddress> transports = new ArrayList<InetSocketAddress>();
        transports.add(new InetSocketAddress(InetAddress.getByName(params.getEsHost()), params.getEsPort()));

        final String esIndex = params.getEsIndex();

        //tumblingwindow.addSink(new ElasticsearchSink<Tuple20<String, String, String, String, String, String, Long, Strin
        // g, String, Boolean, String, String, String, String, Long, String, String, String, String, String>>(config, transports, new ElasticsearchSinkFunction<Tuple20<String, String, String, String, String, String, Long, String, String, Boolean, String, String, String, String, Long, String, String, String, String, String>>() {
        final KeyedStream<Tuple20<String, String, String, String, String, String, Long, String, String, Boolean, String, String, String, String, Long, String, String, String, String, String>, Tuple> ks = source.keyBy(0);
        //final WindowedStream<Tuple20<String, String, String, String, String, String, Long, String, String, Boolean, String, String, String, String, Long, String, String, String, String, String>, Tuple, TimeWindow> window = ks.timeWindow(Time.seconds(10)).trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)));
        //final SingleOutputStreamOperator<Tuple20<String, String, String, String, String, String, Long, String, String, Boolean, String, String, String, String, Long, String, String, String, String, String>> countWindow = source.keyBy(0).countWindow(2000, 1).maxBy(14).name("CountWindow");
        ks.countWindow(1).maxBy(14).addSink(new ElasticsearchSink<Tuple20<String, String, String, String, String, String, Long, String, String, Boolean, String, String, String, String, Long, String, String, String, String, String>>(config, transports, new ElasticsearchSinkFunction<Tuple20<String, String, String, String, String, String, Long, String, String, Boolean, String, String, String, String, Long, String, String, String, String, String>>() {
            public void process(Tuple20<String, String, String, String, String, String, Long, String, String, Boolean, String, String, String, String, Long, String, String, String, String, String> post, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                Map<String, Object> json = new HashMap<String, Object>();
                json.put(DetailedPost.PropertyName.ID, post.f0);
                json.put(DetailedPost.PropertyName.POST_BODY, post.f1);
                json.put(DetailedPost.PropertyName.PROFILE_CITY, post.f2);
                if (post.f3 != null && !post.f3.equals(""))
                    json.put(DetailedPost.PropertyName.CRAWL_DATE, post.f3);
                json.put(DetailedPost.PropertyName.POST_DATE, post.f4);
                json.put(DetailedPost.PropertyName.DIGEST, post.f5);
                json.put(DetailedPost.PropertyName.ENGAGEMENT, post.f6);
                json.put(DetailedPost.PropertyName.PROFILE_GENDER, post.f7);
                json.put(DetailedPost.PropertyName.POST_HREF, post.f8);
                json.put(DetailedPost.PropertyName.IS_COMMENT, post.f9);
                json.put(DetailedPost.PropertyName.PARENT_POST_ID, post.f10);
                json.put(DetailedPost.PropertyName.PROFILE_HREF, post.f11);
                json.put(DetailedPost.PropertyName.PROFILE_ID, post.f12);
                json.put(DetailedPost.PropertyName.PROFILE_NAME, post.f13);
                json.put(DetailedPost.PropertyName.REACH, post.f14);
                json.put(DetailedPost.PropertyName.SCORE, post.f15);
                json.put(DetailedPost.PropertyName.SEGMENT, post.f16);
                json.put(DetailedPost.PropertyName.SM_POST_ID, post.f17);
                json.put(DetailedPost.PropertyName.SM_PROFILE_ID, post.f18);
                json.put(DetailedPost.PropertyName.SOURCE, post.f19);


                final IndexRequest request = Requests.indexRequest()
                        .create(false)
                        .index(esIndex)
                        .type("post")
                        .id(post.f0)
                        .source(json);

                requestIndexer.add(request);
            }
        }));

        streamEnv.execute("SAVE TO ES...");
        //final IterativeDataSet<Tuple2<Post, Profile>> firstIteration = denorm.iterate(5);
        //DataSet<Tuple2<Post, Profile>> firstResult = firstIteration.closeWith(firstIteration);
        //DataSet<Tuple2<Post, Profile>> firstResult = firstIteration.closeWith(firstIteration.map(new IdMapper()));
//
//        final StreamExecutionEnvironment streamEnv = saveToElastic(denorm, params);
//        streamEnv.execute("upload to elastic");

        Long stopTime = System.currentTimeMillis();
        System.out.println("Total posts imported=" + totalPosts);
        long elapsedTime = stopTime - startTime;
        System.out.println("elapsedTime=" + elapsedTime);
    }

    private static StreamExecutionEnvironment saveToElastic(DataSet<PostDetails> collect, final IndexParams.Params params) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final List<PostDetails> posts;
        if (params.getMaxDocs() > 0)
            posts = collect.first(params.getMaxDocs()).collect();
        else
            posts = collect.collect();

        final DataStreamSource<PostDetails> source = env.fromCollection(posts);
        Map<String, String> config = new HashMap<String, String>();
        config.put(ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, params.getMaxActions());
        config.put("cluster.name", params.getEsCluster());

        List<InetSocketAddress> transports = new ArrayList<InetSocketAddress>();
        transports.add(new InetSocketAddress(InetAddress.getByName(params.getEsHost()), params.getEsPort()));

        final String esIndex = params.getEsIndex();
        final AllWindowedStream<PostDetails, GlobalWindow> win = source.countWindowAll(500);


        source.addSink(new ElasticsearchSink<PostDetails>(config, transports, new ElasticsearchSinkFunction<PostDetails>() {
            public void process(PostDetails post, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                Map<String, Object> json = new HashMap<String, Object>();

//                json.put(DetailedPost.PropertyName.ID, post.id);
//                json.put(DetailedPost.PropertyName.CRAWL_DATE, post.crawlDate);
//                json.put(DetailedPost.PropertyName.DIGEST, post.digest);
//                json.put(DetailedPost.PropertyName.SCORE, post.score);
//                json.put(DetailedPost.PropertyName.SEGMENT, post.segment + "-" + profile.segment);
//                json.put(DetailedPost.PropertyName.SOURCE, post.source);
//                json.put(DetailedPost.PropertyName.IS_COMMENT, post.isComment);
//                json.put(DetailedPost.PropertyName.ENGAGEMENT, post.engagement);
//                json.put(DetailedPost.PropertyName.PARENT_POST_ID, post.parentPostId);
//                json.put(DetailedPost.PropertyName.POST_BODY, post.body);
//                json.put(DetailedPost.PropertyName.POST_DATE, post.date);
//                json.put(DetailedPost.PropertyName.POST_HREF, post.href);
//                json.put(DetailedPost.PropertyName.SM_POST_ID, post.smPostId);
//
//                json.put(DetailedPost.PropertyName.PROFILE_CITY, profile.city);
//                json.put(DetailedPost.PropertyName.PROFILE_GENDER, profile.gender);
//                json.put(DetailedPost.PropertyName.PROFILE_HREF, profile.href);
//                json.put(DetailedPost.PropertyName.PROFILE_ID, profile.id);
//                json.put(DetailedPost.PropertyName.PROFILE_NAME, profile.name);
//                json.put(DetailedPost.PropertyName.REACH, profile.reach);
//                json.put(DetailedPost.PropertyName.SM_PROFILE_ID, profile.smProfileId);


                json.put(DetailedPost.PropertyName.ID, post.id);
                json.put(DetailedPost.PropertyName.CRAWL_DATE, post.crawlDate);
                json.put(DetailedPost.PropertyName.DIGEST, post.digest);
                json.put(DetailedPost.PropertyName.SCORE, post.score);
                json.put(DetailedPost.PropertyName.SEGMENT, post.segment);
                json.put(DetailedPost.PropertyName.SOURCE, post.source);
                json.put(DetailedPost.PropertyName.IS_COMMENT, post.isComment);
                json.put(DetailedPost.PropertyName.ENGAGEMENT, post.engagement);
                json.put(DetailedPost.PropertyName.PARENT_POST_ID, post.parentPostId);
                json.put(DetailedPost.PropertyName.POST_BODY, post.body);
                json.put(DetailedPost.PropertyName.POST_DATE, post.date);
                json.put(DetailedPost.PropertyName.POST_HREF, post.href);
                json.put(DetailedPost.PropertyName.SM_POST_ID, post.smPostId);

                json.put(DetailedPost.PropertyName.PROFILE_CITY, post.city);
                json.put(DetailedPost.PropertyName.PROFILE_GENDER, post.gender);
                json.put(DetailedPost.PropertyName.PROFILE_HREF, post.profileHref);
                json.put(DetailedPost.PropertyName.PROFILE_ID, post.profileId);
                json.put(DetailedPost.PropertyName.PROFILE_NAME, post.profileName);
                json.put(DetailedPost.PropertyName.REACH, post.reach);
                json.put(DetailedPost.PropertyName.SM_PROFILE_ID, post.smProfileId);


                final IndexRequest request = Requests.indexRequest()
                        .create(false)
                        .index(esIndex)
                        .type("post")
                        .id(post.id)
                        .source(json);

                requestIndexer.add(request);
            }
        }));
        return env;
    }

    private static void addSegments(Job job, String[] dirs) throws IOException {
        for (int i = 0; i < dirs.length; i++) {
            String segment = dirs[i];
            LOG.info("Add: " + segment);
            SequenceFileInputFormat.addInputPath(job, new Path(segment, CrawlDatum.FETCH_DIR_NAME));
            SequenceFileInputFormat.addInputPath(job, new Path(segment, CrawlDatum.PARSE_DIR_NAME));
            SequenceFileInputFormat.addInputPath(job, new Path(segment, ParseData.DIR_NAME));
            SequenceFileInputFormat.addInputPath(job, new Path(segment, ParseText.DIR_NAME));
        }
    }

    private static void addSegmentDirs(Job job, String[] dirs) throws IOException {
        for (int i = 0; i < dirs.length; i++) {
            Path dir = new Path(dirs[i]);
            FileSystem fs = dir.getFileSystem(job.getConfiguration());
            FileStatus[] fstats = fs.listStatus(dir, HadoopFSUtil.getPassDirectoriesFilter(fs));
            Path[] files = HadoopFSUtil.getPaths(fstats);
            LOG.info("Add dir: " + dir);
            for (Path p : files) {
                if (SegmentChecker.isIndexable(p, fs)) {
                    LOG.info("Add: " + p);
                    SequenceFileInputFormat.addInputPath(job, new Path(p, CrawlDatum.FETCH_DIR_NAME));
                    SequenceFileInputFormat.addInputPath(job, new Path(p, CrawlDatum.PARSE_DIR_NAME));
                    SequenceFileInputFormat.addInputPath(job, new Path(p, ParseData.DIR_NAME));
                    SequenceFileInputFormat.addInputPath(job, new Path(p, ParseText.DIR_NAME));
                }
            }
        }
    }

    public static class PostTuple extends Tuple20<String, String, String, String, String, String, Long, String, String, Boolean, String, String, String, String, Long, String, String, String, String, String> {
        private static final long serialVersionUID = 1L;

        public PostTuple() {
        }

        public PostTuple(
                String id,
                String body,
                String city,
                String crawlDate,
                String date,
                String digest,
                long engagement,
                String gender,
                String href,
                Boolean isComment,
                String parentPostId,
                String profileHref,
                String profileId,
                String profileName,
                long reach,
                String score,
                String segment,
                String smPostId,
                String smProfileId,
                String source) {

            this.setFields(
                    id,
                    body,
                    city,
                    crawlDate,
                    date,
                    digest,
                    engagement,
                    gender,
                    href,
                    isComment,
                    parentPostId,
                    profileHref,
                    profileId,
                    profileName,
                    reach,
                    score,
                    segment,
                    smPostId,
                    smProfileId,
                    source);
//            super(
//                    id,
//                    body,
//                    city,
//                    crawlDate,
//                    date,
//                    digest,
//                    engagement,
//                    gender,
//                    href,
//                    isComment,
//                    parentPostId,
//                    profileHref,
//                    profileId,
//                    profileName,
//                    reach,
//                    score,
//                    segment,
//                    smPostId,
//                    smProfileId,
//                    source);
        }

        public String getId() {
            return this.f0;
        }
    }
}
