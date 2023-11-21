package cn.fudan.cs.stree.engine;

import cn.fudan.cs.input.InputTuple;
import cn.fudan.cs.stree.data.ProductGraph;
import cn.fudan.cs.stree.data.ResultPair;
import cn.fudan.cs.stree.data.AbstractTRDNode;
import cn.fudan.cs.stree.query.Automata;
import com.codahale.metrics.*;

import java.util.Collection;
import java.util.Timer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public abstract class RPQEngine<L> {

    protected MetricRegistry metricRegistry;
    protected Counter resultCounter;
    protected Histogram containingTreeHistogram;
    protected Histogram fullHistogram;
    protected Histogram processedHistogram;
    protected Histogram explicitDeletionHistogram;
    protected Histogram fullProcessedHistogram;
    protected Histogram windowManagementHistogram;
    protected Histogram edgeCountHistogram;
    protected Timer fullTimer;
    protected ProductGraph<Integer, L> productGraph;
    protected Automata<L> automata;
    protected int rank;

    protected ResultPair results;

    protected int edgeCount = 0;


    protected RPQEngine(Automata<L> query, int capacity) {
        automata = query;
//        results = new HashSet<>();
        results = new ResultPair();
        productGraph = new ProductGraph<>(capacity, query);
    }

    public ResultPair getResults() {
        return  results;
    }

    public long getResultCount() {
        return results.getResultSize();
//        return resultCounter.getCount();
    }

    public void addMetricRegistry(MetricRegistry metricRegistry) {
        // register all the matrics
        this.metricRegistry = metricRegistry;

        // a counter that keeps track of total result count
//        this.resultCounter = metricRegistry.counter("result-counter");

        // histogram that keeps track of processing append only  tuples in teh stream if there is a corresponding edge in the product graph
        this.processedHistogram = new Histogram(new SlidingTimeWindowArrayReservoir(10, TimeUnit.MINUTES));
        metricRegistry.register("processed-histogram", this.processedHistogram);

        // histogram that keeps track of processing of explicit negative tuples
        this.explicitDeletionHistogram = new Histogram(new SlidingTimeWindowArrayReservoir(10, TimeUnit.MINUTES));
        metricRegistry.register("explicit-deletion-histogram", this.explicitDeletionHistogram);

        // histogram responsible of tracking how many trees are affected by each input stream edge
        this.containingTreeHistogram = new Histogram(new SlidingTimeWindowArrayReservoir(10, TimeUnit.MINUTES));
        metricRegistry.register("containing-tree-counter", this.containingTreeHistogram);

        // measures the time spent on processing each edge from the input stream
//        this.fullTimer = new Timer(new SlidingTimeWindowArrayReservoir(10, TimeUnit.MINUTES));
//        metricRegistry.register("full-timer", this.fullTimer);

        // histogram responsible to measure time spent in Window Expiry procedure at every slide interval
        this.windowManagementHistogram = new Histogram(new SlidingTimeWindowArrayReservoir(10, TimeUnit.MINUTES));
        metricRegistry.register("window-histogram", windowManagementHistogram);

        // histogram responsible of keeping track number of edges in each side of a window
        edgeCountHistogram = metricRegistry.histogram("edgecount-histogram");

        this.productGraph.addMetricRegistry(metricRegistry);
    }

    public abstract Collection<AbstractTRDNode>[] processEdge(InputTuple<Integer, Integer, L> inputTuple) throws ExecutionException, InterruptedException;

    public abstract void GC(long minTimestamp);
    public abstract void shutDown();

    public abstract void record(long ts);
}
