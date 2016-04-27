package ru.wobot.index.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
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
        if (args.length < 1) {
            System.err.println("Usage: <segment> ... | --dir <segments> ...");
            throw new RuntimeException();
        }

        long startTime = System.currentTimeMillis();
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final Job profileMapJob = Job.getInstance();

        addFiles(profileMapJob, args);

        //profileMapJob.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, HBaseConstants.PROFILE_TABLE_NAME);
        //profileMapJob.getConfiguration().set("mapreduce.output.fileoutputformat.outputdir", HBaseConstants.TMP_DIR);
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

        FlatJoinFunction<Tuple2<Text, Post>, Tuple2<Text, Profile>, Tuple2<Post, Profile>> join = new FlatJoinFunction<Tuple2<Text, Post>, Tuple2<Text, Profile>, Tuple2<Post, Profile>>() {
            public void join(Tuple2<Text, Post> post, Tuple2<Text, Profile> profile, Collector<Tuple2<Post, Profile>> collector) throws Exception {
                collector.collect(Tuple2.of(post.f1, profile.f1));
            }
        };
        DataSet<Tuple2<Post, Profile>> denorm = posts.join(profiles).where(0).equalTo(0).with(join);
        final List<Tuple2<Post, Profile>> collect = denorm.collect();
        saveToElastic(collect);

        Long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        System.out.println("Total posts imported=" + collect.size());
        System.out.println("elapsedTime=" + elapsedTime);
    }

    private static void saveToElastic(List<Tuple2<Post, Profile>> collect) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStreamSource<Tuple2<Post, Profile>> source = env.fromCollection(collect);
        Map<String, String> config = new HashMap<String, String>();
        //config.put("bulk.flush.max.actions", "1");
        config.put("cluster.name", "kviz-es");

        List<InetSocketAddress> transports = new ArrayList<InetSocketAddress>();
        transports.add(new InetSocketAddress(InetAddress.getByName("192.168.1.101"), 9300));


        source.addSink(new ElasticsearchSink<Tuple2<Post, Profile>>(config, transports, new ElasticsearchSinkFunction<Tuple2<Post, Profile>>() {
            public void process(Tuple2<Post, Profile> element, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                Map<String, Object> json = new HashMap<String, Object>();
                final Post post = element.f0;
                json.put("body", post.body);


                final IndexRequest request = Requests.indexRequest()
                        .index("wn")
                        .type("post")
                        .source(json)
                        .id(post.id);
                requestIndexer.add(request);
            }
        }));

        env.execute("upload to elastic");
    }

    private static void addFiles(Job job, String[] args) throws IOException {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--dir")) {
                Path dir = new Path(args[++i]);
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
            } else {
                String segment = args[i];
                LOG.info("Add: " + segment);
                SequenceFileInputFormat.addInputPath(job, new Path(segment, CrawlDatum.FETCH_DIR_NAME));
                SequenceFileInputFormat.addInputPath(job, new Path(segment, CrawlDatum.PARSE_DIR_NAME));
                SequenceFileInputFormat.addInputPath(job, new Path(segment, ParseData.DIR_NAME));
                SequenceFileInputFormat.addInputPath(job, new Path(segment, ParseText.DIR_NAME));
            }
        }
    }

}
