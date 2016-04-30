package ru.wobot.index.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.SampleInPartition;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormat;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.wobot.index.DetailedPost;
import ru.wobot.index.Post;
import ru.wobot.index.Profile;
import ru.wobot.index.Types;

import java.io.IOException;
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

        FlatJoinFunction<Tuple2<Text, Post>, Tuple2<Text, Profile>, PostDetaild> join = new FlatJoinFunction<Tuple2<Text, Post>, Tuple2<Text, Profile>, PostDetaild>() {
            public void join(Tuple2<Text, Post> tp, Tuple2<Text, Profile> ta, Collector<PostDetaild> collector) throws Exception {
                final Post post = tp.f1;
                final Profile profile = ta.f1;
                final PostDetaild result = new PostDetaild();
                result.id=post.id;
                result.crawlDate=post.crawlDate;
                result.digest=post.digest;
                result.score=post.score;
                result.segment=post.segment + "-" + profile.segment;
                result.source=post.source;
                result.isComment=post.isComment;
                result.engagement=post.engagement;
                result.parentPostId=post.parentPostId;
                result.body=post.body;
                result.date=post.date;
                result.href=post.href;
                result.smPostId=post.smPostId;

                result.city=profile.city;
                result.gender=profile.gender;
                result.profileHref=profile.href;
                result.profileId=profile.id;
                result.profileName=profile.name;
                result.reach=profile.reach;
                result.smProfileId=profile.smProfileId;

                collector.collect(result);
            }
        };
        DataSet<PostDetaild> denorm = posts.join(profiles, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where(0).equalTo(0).with(join).rebalance();
        //final IterativeDataSet<Tuple2<Post, Profile>> firstIteration = denorm.iterate(5);
        //DataSet<Tuple2<Post, Profile>> firstResult = firstIteration.closeWith(firstIteration);
        //DataSet<Tuple2<Post, Profile>> firstResult = firstIteration.closeWith(firstIteration.map(new IdMapper()));

        //final List<Tuple2<Post, Profile>> collect = denorm.collect();
        //firstResult.print();

        saveToElastic(denorm, params);

        Long stopTime = System.currentTimeMillis();
        //System.out.println("Total posts imported=" + collect.size());
        long elapsedTime = stopTime - startTime;
        System.out.println("elapsedTime=" + elapsedTime);
    }

    private static void saveToElastic(DataSet<PostDetaild> collect, final IndexParams.Params params) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStreamSource<PostDetaild> source = env.fromCollection(collect.collect());
        Map<String, String> config = new HashMap<String, String>();
        config.put("bulk.flush.max.actions", "1");
        config.put("cluster.name", params.getEsCluster());

        List<InetSocketAddress> transports = new ArrayList<InetSocketAddress>();
        transports.add(new InetSocketAddress(InetAddress.getByName(params.getEsHost()), params.getEsPort()));

        final String esIndex = params.getEsIndex();
        source.addSink(new ElasticsearchSink<PostDetaild>(config, transports, new ElasticsearchSinkFunction<PostDetaild>() {
            public void process(PostDetaild post, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
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
                        .source(json)
                        .id(post.id);
                requestIndexer.add(request);
            }
        }));

        env.execute("upload to elastic");
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

    private static class IdMapper implements org.apache.flink.api.common.functions.MapFunction<Tuple2<Post, Profile>, Tuple2<Post, Profile>> {
        public Tuple2<Post, Profile> map(Tuple2<Post, Profile> postProfileTuple2) throws Exception {
            return postProfileTuple2;
        }
    }
}
