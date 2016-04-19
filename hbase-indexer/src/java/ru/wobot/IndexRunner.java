package ru.wobot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexRunner {
    public static final Logger LOG = LoggerFactory
            .getLogger(IndexRunner.class);

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: <crawldb> (<segment> ... | -dir <segments>)");
            throw new RuntimeException();
        }
    }
}
