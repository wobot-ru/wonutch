package ru.wobot.flink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TypeSerializerInputFormat;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;

public class StreamRunner {
    public static final Logger LOG = LoggerFactory
            .getLogger(StreamRunner.class);

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: <segment> ... | --dir <segments> ...");
            throw new RuntimeException();
        }
        long startTime = System.currentTimeMillis();

        run(args);

        Long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        System.out.println("elapsedTime=" + elapsedTime);
    }

    private static void run(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStreamSource<Long> input  = env.generateSequence(0, 100);
        Map<String, String> config = new HashMap<String, String>();
        //config.put("bulk.flush.max.actions", "1");
        config.put("cluster.name", "kviz-es");

        List<InetSocketAddress> transports = new ArrayList<InetSocketAddress>();
        transports.add(new InetSocketAddress(InetAddress.getByName("192.168.1.101"), 9300));


        input.addSink(new ElasticsearchSink<Long>(config, new IndexRequestBuilder<Long>() {
            public IndexRequest createIndexRequest(Long element, RuntimeContext ctx) {
                Map<String, Object> json = new HashMap<String, Object>();
                json.put("data", element);

                return Requests.indexRequest()
                        .index("m")
                        .type("my-type")
                        .source(json)
                        .id(String.valueOf(new Random().nextInt()));
            }
        }));

        env.execute("Elasticsearch Example");
    }

    private static void run2(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStreamSource<Long> input  = env.generateSequence(0, 100);
        Map<String, String> config = new HashMap<String, String>();
//        config.put("bulk.flush.max.actions", "1");
        config.put("cluster.name", "kviz-es");

        List<InetSocketAddress> transports = new ArrayList<InetSocketAddress>();
        transports.add(new InetSocketAddress(InetAddress.getByName("192.168.1.101"), 9300));


        input.addSink(new ElasticsearchSink<Long>(config, new IndexRequestBuilder<Long>() {
            public IndexRequest createIndexRequest(Long element, RuntimeContext ctx) {
                Map<String, Object> json = new HashMap<String, Object>();
                json.put("data", element);

                return Requests.indexRequest()
                        .index("m")
                        .type("my-type")
                        .source(json)
                        .id(String.valueOf(new Random().nextInt()));
            }
        }));

        env.execute("Elasticsearch Example");
    }
}
