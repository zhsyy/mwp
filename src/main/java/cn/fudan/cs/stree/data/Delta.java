package cn.fudan.cs.stree.data;

import cn.fudan.cs.stree.util.Constants;
import cn.fudan.cs.stree.util.Hasher;
import cn.fudan.cs.stree.util.RunningSnapShot;
import com.codahale.metrics.MetricRegistry;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class Delta<V, T extends AbstractTRD<V, T, N>, N extends AbstractTRDNode<V, T, N>>{

    /* A root node corresponds to a tree */
    public ConcurrentHashMap<V, T> trdIndex;
    /* one node corresponds to a tree set containing this node. */
    public ConcurrentHashMap<Hasher.MapKey<V>, Set<T>> nodeToTRDIndex;

    public AtomicLong nodeCount = new AtomicLong(0);

    private ObjectFactory<V, T, N> objectFactory;

    public Delta(int capacity, ObjectFactory<V, T, N> objectFactory) {
        trdIndex = new ConcurrentHashMap<>(Constants.EXPECTED_TREES);
        nodeToTRDIndex = new ConcurrentHashMap<>(Constants.EXPECTED_TREES);
        this.objectFactory = objectFactory;
    }

    public void record(){
        RunningSnapShot.addTRDCount(trdIndex.size());
        RunningSnapShot.addNodeCount(nodeCount.get());
    }

    public ObjectFactory<V, T, N> getObjectFactory() {
        return objectFactory;
    }

    public Collection<T> getTrees(V vertex, int state) {
        return nodeToTRDIndex.computeIfAbsent(Hasher.createTreeNodePairKey(vertex, state), key -> Collections.newSetFromMap(new ConcurrentHashMap<T, Boolean>()) );
    }

    public boolean exists(V vertex, int state, V root) {
        return trdIndex.containsKey(vertex);
    }

    /* create a new tree with a single root node (ts is set to the basic model) */
    public T addLocalTree(V vertex, long edge_time) {
        T tree = objectFactory.createTRD(this, vertex, 0, vertex, edge_time);
        trdIndex.put(vertex, tree);
        addToTreeNodeIndex(tree, tree.getRootNode());
        return tree;
    }

    public void removeTree(T tree) {
        this.trdIndex.remove(tree.getRootVertex());
        removeFromTreeIndex(tree.rootNode, tree);
    }

    public void addToTreeNodeIndex(T tree, N treeNode) {
        nodeCount.incrementAndGet();
        Collection<T> containingTrees = getTrees(treeNode.getVertex(), treeNode.getState());
        containingTrees.add(tree);
    }

    public void removeFromTreeIndex(N removedNode, T tree) {
        Collection<T> containingTrees = this.nodeToTRDIndex.get(Hasher.getThreadLocalTreeNodePairKey(removedNode.getVertex(), removedNode.getState()));
        if (containingTrees != null) {
            nodeCount.decrementAndGet();
            containingTrees.remove(tree);
            if (containingTrees.size() == 0)
                this.nodeToTRDIndex.remove(Hasher.getThreadLocalTreeNodePairKey(removedNode.getVertex(), removedNode.getState()));
        }
    }

    public <L> void DGC(Long currentTimestamp, Long windowSize, ExecutorService executorService) {
        Collection<T> trees = trdIndex.values();
//        int size = 0;
        CompletionService<Integer> completionService = new ExecutorCompletionService<>(executorService);

        for(T tree : trees) {
            if (tree.getMinTimestamp() > currentTimestamp - windowSize) {
                continue;
            }
            RAPQTRDGCJob<V, L, T> RAPQTRDGCJob = new RAPQTRDGCJob<>(currentTimestamp, tree);
            completionService.submit(RAPQTRDGCJob);
        }
    }

    public void addMetricRegistry(MetricRegistry metricRegistry) {
//        this.treeCounter = metricRegistry.counter("tree-counter");
//        this.treeSizeHistogram = metricRegistry.histogram("tree-size-histogram");
    }

    private static class RAPQTRDGCJob<V,L, T extends AbstractTRD> implements Callable<Integer> {

        private final long minTimestamp;
        private T trd;

        public RAPQTRDGCJob(Long minTimestamp, T trd) {
            this.minTimestamp = minTimestamp;
            this.trd = trd;
        }

        @Override
        public Integer call() {
            int ret =  trd.removeOldEdges(minTimestamp);
            if (trd.nodeIndex.keySet().size() == 1) {
                trd.delta.removeTree(trd);
            }
            return ret;
        }
    }
}
