package cn.fudan.cs.stree.util;

public class Constants {


    // average number of neighbours per node
    public static final int EXPECTED_NEIGHBOURS = 12;

    // expected number of nodes in the productGraph
    public static final int EXPECTED_NODES = 50000000;

    // expected number of tree a single edge is in
    public static final int EXPECTED_TREES = 65536;

    // expected number of labels in a productGraph
    public static final int EXPECTED_LABELS = 8;

    /**
     * Total number of SpanningTreeExpansion jobs to be assigned to a thread
     */
    public static final int EXPECTED_BATCH_SIZE = 2;

    public static final int EXPECTED_TREE_SIZE = 8192;

    public static final int HISTOGRAM_BUCKET_SIZE = 526336;

    public static final char REVERSE_LABEL_SYMBOL = '^';

    public static final String EPSILON_TRANSITION = "EPSILON";

    public static final int TUPLES = 1;

    public static final int CWV2WORKERS = 2;

    public static final int EXPIRATION_TIME = 3;

    public static final int EXPIRATION_REQUEST = 4;

    public static final int NODES = 5;

    public static final int FINISH = 7;

    public static final int VOTE = 6;

    public static final int METRIC = 10;
}
