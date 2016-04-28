package ru.wobot.index.flink;


import org.apache.commons.cli.*;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class IndexParams {
    private final static String PROPERTY_FILE_NAME = "wn-index.properties";
    private final static String HELP_OP = "?";
    private final static String HELP_LONG_OP = "help";
    private final static String SEG_OP = "s";
    private final static String DIR_OP = "d";
    private final static String ES_HOST_KEY = "elastic.host";
    private final static String ES_PORT_KEY = "elastic.port";
    private final static String ES_CLUSTER_KEY = "elastic.cluster";
    private final static String ES_INDEX_KEY = "elastic.index";

    public static Params parse(String[] args) {
        boolean showHelp = false;
        for (int i = 0; i < args.length; i++)
            if (args[i].equals("-" + HELP_OP) || args[i].equals("-" + HELP_LONG_OP)) {
                showHelp = true;
                break;
            }


        final Options options = new Options();
        options.addOption(Option.builder(HELP_OP).longOpt(HELP_LONG_OP).desc("print this help message").build());
        OptionGroup optionGroup = new OptionGroup();
        optionGroup.setRequired(true);
        optionGroup.addOption(Option
                .builder(SEG_OP)
                .longOpt("seg")
                .argName("segment")
                .numberOfArgs(Option.UNLIMITED_VALUES)
                .desc("indexing nutch's segments")
                .build());
        optionGroup.addOption(Option
                .builder(DIR_OP)
                .longOpt("dir")
                .argName("segments")
                .numberOfArgs(Option.UNLIMITED_VALUES)
                .desc("indexing directories of nutch segments")
                .build());
        options.addOptionGroup(optionGroup);

        if (showHelp){
            HelpFormatter f = new HelpFormatter();
            f.printHelp("wn-indexer", options, true);
            return new Params();
        }
        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);
            String esHost = null;
            int esPort = -1;
            String esCluster = null;
            String esIndex = null;
            try {
                Configuration config = new PropertiesConfiguration(PROPERTY_FILE_NAME);
                if (!config.containsKey(ES_HOST_KEY)) {
                    throw new RuntimeException(ES_HOST_KEY + " not defined in " + PROPERTY_FILE_NAME);
                }
                if (!config.containsKey(ES_PORT_KEY)) {
                    throw new RuntimeException(ES_PORT_KEY + " not defined in " + PROPERTY_FILE_NAME);
                }
                if (!config.containsKey(ES_CLUSTER_KEY)) {
                    throw new RuntimeException(ES_CLUSTER_KEY + " not defined in " + PROPERTY_FILE_NAME);
                }
                if (!config.containsKey(ES_INDEX_KEY)) {
                    throw new RuntimeException(ES_INDEX_KEY + " not defined in " + PROPERTY_FILE_NAME);
                }

                esHost = config.getString(ES_HOST_KEY);
                esPort = config.getInt(ES_PORT_KEY);
                esCluster = config.getString(ES_CLUSTER_KEY);
                esIndex = config.getString(ES_INDEX_KEY);
            } catch (ConfigurationException e) {
                e.printStackTrace();
            }
            return new Params(cmd.getOptionValues(SEG_OP), cmd.getOptionValues(DIR_OP), esHost, esPort, esCluster, esIndex);

        } catch (ParseException ex) {
            HelpFormatter f = new HelpFormatter();
            f.printHelp("wn-indexer", options, true);
            System.out.println();
            System.out.println(ex.getMessage());
        }
        return new Params();
    }

    static class Params {
        private final String[] segs;
        private final String[] dirs;
        private final String esHost;
        private final int esPort;
        private final String esCluster;
        private final String esIndex;
        private final boolean canExecute;

        private Params() {
            segs = null;
            dirs = null;
            esIndex = null;
            esPort = -1;
            esHost = null;
            esCluster = null;
            canExecute = false;
        }

        public Params(String[] segs, String[] dirs, String esHost, int esPort, String esCluster, String esIndex) {
            this.segs = segs;
            this.dirs = dirs;
            this.esHost = esHost;
            this.esPort = esPort;
            this.esCluster = esCluster;
            this.esIndex = esIndex;
            canExecute = true;
        }

        public String[] getDirs() {
            return dirs;
        }

        public boolean canExecute() {
            return canExecute;
        }

        public String[] getSegs() {
            return segs;
        }

        public String getEsHost() {
            return esHost;
        }

        public int getEsPort() {
            return esPort;
        }

        public String getEsCluster() {
            return esCluster;
        }

        public String getEsIndex() {
            return esIndex;
        }
    }
}
