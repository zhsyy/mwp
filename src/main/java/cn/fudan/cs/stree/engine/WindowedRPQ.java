package cn.fudan.cs.stree.engine;

import cn.fudan.cs.input.InputTuple;
import cn.fudan.cs.stree.data.AbstractTRD;
import cn.fudan.cs.stree.data.AbstractTRDNode;
import cn.fudan.cs.stree.data.Delta;
import cn.fudan.cs.stree.data.ObjectFactory;
import cn.fudan.cs.stree.data.arbitrary.ObjectFactoryArbitrary;
import cn.fudan.cs.stree.util.RunningSnapShot;
import cn.fudan.cs.stree.util.Semantics;
import cn.fudan.cs.stree.query.Automata;
import cn.fudan.cs.stree.util.Hasher;
import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.*;

public class WindowedRPQ<L, T extends AbstractTRD<Integer, T, N>, N extends AbstractTRDNode<Integer, T, N>> extends RPQEngine<L> {

    private final long windowSize;
    // Default 0
    private final long step;
    private Semantics semantics;
    public Delta<Integer, T, N> delta;
    ObjectFactory<Integer, T, N> objectFactory;
    private ExecutorService executorService;
    private int numOfThreads;
    public ConcurrentLinkedDeque<Future<?>> futureList;
    public CompletionService<?> completionService;
    public AbstractTRDExpansionJob treeExpansionJob;
    private boolean collectionDoneTaskFlag = true;
    private static Logger logger = LoggerFactory.getLogger(WindowedRPQ.class);

    /**
     * Windowed RPQ engine ready to process edges
     * @param query Automata representation of the persistent query
     * @param capacity Initial size for internal data structures. Set to approximate number of edges in a window
     * @param windowSize Size of the sliding window in milliseconds
     * @param step Slide interval in milliseconds
     * @param numOfThreads Total number of executor threads
     * @param semantics Resulting path semantics: @{@link Semantics}
     */
    public WindowedRPQ(Automata<L> query, int capacity, long windowSize, long step, int numOfThreads, Semantics semantics) {
        super(query, capacity);
        if (semantics.equals(Semantics.ARBITRARY)) {
            this.objectFactory = new ObjectFactoryArbitrary();
        } else {
//            this.objectFactory = new ObjectFactorySimple();
        }
        this.delta = new Delta<>(capacity, objectFactory);
        this.windowSize = windowSize;
        this.step = step;
        this.executorService = Executors.newFixedThreadPool(numOfThreads);
        this.numOfThreads = numOfThreads;
        this.semantics = semantics;
        futureList = new ConcurrentLinkedDeque<>();
        completionService = new ExecutorCompletionService<>(this.executorService);
        treeExpansionJob = objectFactory.createExpansionJob(productGraph, automata, results, windowSize, step, false);
        new Thread(() -> {
            while(collectionDoneTaskFlag) {
                if (!futureList.isEmpty() && futureList.getFirst().isDone()){
                    futureList.removeFirst();
                }
            }
        }).start();
    }


    @Override
    public void addMetricRegistry(MetricRegistry metricRegistry) {
        this.delta.addMetricRegistry(metricRegistry);
        // call super function to include all other histograms
        super.addMetricRegistry(metricRegistry);
    }


    @Override
    public Collection<AbstractTRDNode>[] processEdge(InputTuple<Integer, Integer, L> inputTuple) throws ExecutionException, InterruptedException {
        long currentTimestamp = inputTuple.getTimestamp();

        Map<Integer, Integer> transitions = automata.getTransition(inputTuple.getLabel());

        if(transitions.isEmpty()) {
            // there is no transition with given label, simply return
            return null;
        } else {
            // add edge to the snapshot productGraph
            if (inputTuple.isDeletion())
                productGraph.removeEdge(inputTuple.getSource(), inputTuple.getTarget(), inputTuple.getLabel(), inputTuple.getTimestamp());
            else
                productGraph.addEdge(inputTuple.getSource(), inputTuple.getTarget(), inputTuple.getLabel(), inputTuple.getTimestamp());
        }

        // edge is an insertion
        //create a spanning tree for the source node in case it does not exists
        if (transitions.containsKey(0) && !delta.exists(inputTuple.getSource(), 0, inputTuple.getSource())) {
            // if there exists a start transition with given label, there should be a spanning tree rooted at source vertex
            delta.addLocalTree(inputTuple.getSource(), currentTimestamp);
        }

        List<Map.Entry<Integer, Integer>> transitionList = new ArrayList<>(transitions.entrySet());

        // for each transition that given label satisfy
        for (Map.Entry<Integer, Integer> transition : transitionList) {
            int sourceState = transition.getKey();
            int targetState = transition.getValue();

            if (!delta.nodeToTRDIndex.containsKey(Hasher.getThreadLocalTreeNodePairKey(inputTuple.getSource(), sourceState)))
                continue;
            Collection<T> containingTrees = delta.getTrees(inputTuple.getSource(), sourceState);

            for (T spanningTree : containingTrees) {
                // we do not check target here as even if it exists, we might update its timestamp
                N parentNode = spanningTree.getNodes(inputTuple.getSource(), sourceState);
                if (parentNode != null && parentNode.checkIfMerge(currentTimestamp, windowSize))
                    continue;

                treeExpansionJob.addJob(spanningTree, parentNode, inputTuple.getTarget(), targetState,
                        inputTuple.getTimestamp(), inputTuple.getTimestamp());
                // check whether the job is full and ready to submit
                if (treeExpansionJob.isFull()) {
                    futureList.add(completionService.submit(treeExpansionJob));
                    treeExpansionJob = objectFactory.createExpansionJob(productGraph, automata,
                            results, windowSize, step, inputTuple.isDeletion());
                }
            }
        }
        return null;
    }

    public void processFinish() {
        try {
            collectionDoneTaskFlag = false;
            for (int i = 0; i < futureList.size(); i++) {
                try {
                    completionService.take().get();
                } catch (InterruptedException | ExecutionException e) {
                    logger.error("TRD Expansion interrupted during execution", e);
                }
            }
            if (!treeExpansionJob.isEmpty()) {
                treeExpansionJob.call();
            }
        } catch (Exception e){
            e.printStackTrace();
        }
        futureList.clear();
    }

    @Override
    public void shutDown() {
        processFinish();
        // shutdown executors
        this.executorService.shutdown();
        this.results.shutDown();
    }
    @Override
    public void record(long ts){
        RunningSnapShot.addRecordTime();
        delta.record();
        productGraph.record();
        RunningSnapShot.addMemUse(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
    }

    /**
     * updates Delta and Spanning Trees and removes any node that is lower than the window endpoint
     * might need to traverse the entire spanning tree to make sure that there does not exists an alternative path
     */
    @Override
    public void GC(long minTimestamp) {
        logger.info("start to GC");
        while (!futureList.isEmpty()){
        }

        productGraph.removeOldEdges(minTimestamp - windowSize);
        delta.DGC(minTimestamp, windowSize, this.executorService);
    }
}
