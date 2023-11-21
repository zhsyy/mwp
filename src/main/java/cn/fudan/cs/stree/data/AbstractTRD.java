package cn.fudan.cs.stree.data;

import cn.fudan.cs.stree.data.arbitrary.TRDRAPQ;
import cn.fudan.cs.stree.util.Constants;
import cn.fudan.cs.stree.util.RunningSnapShot;
import cn.fudan.cs.stree.util.Hasher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractTRD<V, T extends AbstractTRD<V, T, N>, N extends AbstractTRDNode<V, T, N>> {
    protected final Logger LOG = LoggerFactory.getLogger(TRDRAPQ.class);

    protected V root_vertex;
    protected N rootNode;
    protected Delta<V, T, N> delta;

    //一个key对应一个node的collection
    public ConcurrentHashMap<Hasher.MapKey<V>, N> nodeIndex;

    public ConcurrentHashMap<Long, HashSet<Hasher.MapKey<V>>> leastTs2NodeList;
    /* The maximum value of all successfully inserted edges.
     The role of the two ts here is to filter trees that do not need to be modified,
     especially for new edge insertion and expiration operations. */
    protected long maxTimestamp = 0;
    protected long minTimestamp = Long.MAX_VALUE;

    protected AbstractTRD(Delta<V, T, N> delta, long edgeTimestamp) {

        this.nodeIndex = new ConcurrentHashMap<>(Constants.EXPECTED_TREE_SIZE, Constants.EXPECTED_LABELS);
        this.leastTs2NodeList = new ConcurrentHashMap<>();
        this.delta = delta;
    }

    public int getSize() {
        return nodeIndex.size();
    }

    public N addNode(AbstractTRDNode parentNode, V childVertex, int childState, long edgeTimestamp, long windowSize, long step) {
        N child = delta.getObjectFactory().createTRDNode((T) this, childVertex, childState);
        child.calculateAndUpdateTimestamps(edgeTimestamp, windowSize, step, parentNode);
        // Successfully added a new node
        if (!child.upper_bound_timestamp.isEmpty()){
            nodeIndex.put(Hasher.createTreeNodePairKey(childVertex, childState), child);
            this.delta.addToTreeNodeIndex((T) this, child);
            return child;
        }
        return null;
    }

    public void removeNode(N node){
        nodeIndex.remove(Hasher.getThreadLocalTreeNodePairKey(node.getVertex(), node.getState()));
        delta.removeFromTreeIndex(node, (T) this);
    }

    /**
     * removes old edges from the productGraph, used during window management.
     * This function assumes that expired edges are removed from the productGraph, so traversal assumes that it is guarenteed to
     * traverse valid edges
     * @param minTimestamp lower bound of the window interval. Any edge whose timestamp is smaller will be removed
     * @return The set of nodes that have expired from the window as there is no other path
     */
    public int removeOldEdges(long minTimestamp) {
        int unExpiredCount = 0;
        Collection<N> nodes = nodeIndex.values();
        long newMinTimestamp = Long.MAX_VALUE;
        for (N node : nodes) {
            long removeTs = node.lower_bound_timestamp.getFirst();
            if (node.upper_bound_timestamp.getLast() < minTimestamp){
                nodeIndex.remove(Hasher.getThreadLocalTreeNodePairKey(node.getVertex(), node.getState()));
                delta.removeFromTreeIndex(node, (T) this);
                unExpiredCount++;
                RunningSnapShot.labeledTs2NodeCount.put(removeTs,
                        RunningSnapShot.labeledTs2NodeCount.get(removeTs)-1);
            } else {
                while (node.upper_bound_timestamp.getFirst() < minTimestamp){
                    node.upper_bound_timestamp.removeFirst();
                    node.lower_bound_timestamp.removeFirst();
                }

                RunningSnapShot.labeledTs2NodeCount.put(removeTs,
                        RunningSnapShot.labeledTs2NodeCount.get(removeTs)-1);
                long addTs = node.lower_bound_timestamp.getFirst();
                RunningSnapShot.labeledTs2NodeCount.put(addTs,
                        RunningSnapShot.labeledTs2NodeCount.get(addTs)+1);

//                if (node.lower_bound_timestamp.getFirst() > minTimestamp)

                long currentMin = node.upper_bound_timestamp.getFirst();
                if (currentMin > minTimestamp && currentMin < newMinTimestamp)
                    newMinTimestamp = currentMin;
            }
        }
        this.minTimestamp = newMinTimestamp;
        return unExpiredCount;
    }

    public boolean exists(V vertex, int state) {
        return nodeIndex.containsKey(Hasher.getThreadLocalTreeNodePairKey(vertex, state));
    }

    public N getNodes(V vertex, int state) {
        return nodeIndex.get(Hasher.getThreadLocalTreeNodePairKey(vertex, state));
    }

    public V getRootVertex() {
        return this.root_vertex;
    }

    public N getRootNode() {
        return this.rootNode;
    }

    public void setMaxTimestamp(long maxTimestamp) {
        synchronized (this) {
            if (this.maxTimestamp < maxTimestamp)
                this.maxTimestamp = maxTimestamp;
        }
    }

    public void setMinTimestamp(long minTimestamp) {
        synchronized (this) {
            if (this.minTimestamp > minTimestamp)
                this.minTimestamp = minTimestamp;
        }
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }
    public long getMinTimestamp() {
        return minTimestamp;
    }
    @Override
    public String toString() {
        return "root : " + rootNode + " - labeled ts :" + rootNode.getLabeledTimestamp();
    }
}
