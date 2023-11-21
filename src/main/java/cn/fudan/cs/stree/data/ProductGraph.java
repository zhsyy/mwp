package cn.fudan.cs.stree.data;

import cn.fudan.cs.stree.query.Automata;
import cn.fudan.cs.stree.util.Hasher;
import cn.fudan.cs.stree.util.RunningSnapShot;
import com.codahale.metrics.MetricRegistry;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ProductGraph<V,L> {

    private Automata<L> automata;

    //A vertex+state locates a node separately
    private ConcurrentHashMap<Hasher.MapKey<V>, ProductGraphNode<V>> nodeIndex;

    private int edgeCount;

    private LinkedList<GraphEdge<ProductGraphNode<V>>> timeOrderedEdges;

//    private final Logger LOG = LoggerFactory.getLogger(ProductGraph.class);

    //增加capacity作用是避免空间浪费，在map的size溢出后会自动扩容
    public ProductGraph(int capacity, Automata<L> automata) {
        timeOrderedEdges = new LinkedList<GraphEdge<ProductGraphNode<V>>>();
        nodeIndex = new ConcurrentHashMap<>(capacity);
        this.automata = automata;
        this.edgeCount = 0;
    }

    public void record(){
        RunningSnapShot.addEdgeCount(edgeCount);
    }

    public void addEdge(V source, V target, L label, long timestamp) {
        Map<Integer, Integer> transitions = automata.getTransition(label);
        for(Map.Entry<Integer, Integer> transition : transitions.entrySet()) {
            int sourceState = transition.getKey();
            int targetState = transition.getValue();
            ProductGraphNode<V> sourceNode = this.getNode(source, sourceState);
            ProductGraphNode<V> targetNode = this.getNode(target, targetState);
            GraphEdge<ProductGraphNode<V>> forwardEdge = new GraphEdge<>(sourceNode, targetNode, timestamp);
            sourceNode.addForwardEdge(forwardEdge);
            targetNode.addBackwardEdge(forwardEdge);
            timeOrderedEdges.add(forwardEdge);
            edgeCount++;
        }
    }

    public void removeEdge(V source, V target, L label, long timestamp) {
        Map<Integer, Integer> transitions = automata.getTransition(label);
        for(Map.Entry<Integer, Integer> transition : transitions.entrySet()) {
            int sourceState = transition.getKey();
            int targetState = transition.getValue();
            ProductGraphNode<V> sourceNode = this.getNode(source, sourceState);
            ProductGraphNode<V> targetNode = this.getNode(target, targetState);
            GraphEdge<ProductGraphNode<V>> forwardEdge = new GraphEdge<>(sourceNode, targetNode, timestamp);
            sourceNode.removeForwardEdge(forwardEdge);
            targetNode.removeBackwardEdge(forwardEdge);
            edgeCount--;
        }
    }


        private ProductGraphNode<V> getNode(V vertex, int state) {
        ProductGraphNode<V> node = this.nodeIndex.get(Hasher.getThreadLocalTreeNodePairKey(vertex, state));
        if(node == null) {
            node = new ProductGraphNode<>(vertex, state);
            this.nodeIndex.put(Hasher.createTreeNodePairKey(vertex, state), node);
        }
        return node;
    }


    //出边入边都存了一遍，一个存了forward，另一个存了backward
    private void removeEdgeFromHashIndexes(GraphEdge<ProductGraphNode<V>> edge) {
        edge.getSource().removeForwardEdge(edge);
        edge.getTarget().removeBackwardEdge(edge);
    }


    public Collection<GraphEdge<ProductGraphNode<V>>> getForwardEdges(V source, int state) {
        return this.getNode(source, state).getForwardEdges();
    }

    public Collection<GraphEdge<ProductGraphNode<V>>> getBackwardEdges(V source, int state) {
        return this.getNode(source, state).getBackwardEdges();
    }

    /**
     * removes old edges from the productGraph, used during window management
     * @param minTimestamp lower bound of the window interval. Any edge whose timestamp is smaller will be removed
     */
    public void removeOldEdges(long minTimestamp) {
//        LOG.info("Graph expiry at {}", minTimestamp);
        // it suffices to linearly scan from the oldest edge as we assume ordered arrival
        Iterator<GraphEdge<ProductGraphNode<V>>> edgeIterator = timeOrderedEdges.iterator();
        while(edgeIterator.hasNext()) {
            GraphEdge<ProductGraphNode<V>> oldestEdge = edgeIterator.next();
            if(oldestEdge.getTimestamp() <= minTimestamp) {
                edgeIterator.remove();
                removeEdgeFromHashIndexes(oldestEdge);
                edgeCount--;
            } else {
                // as we assume ordered arrival, we can stop the search
                break;
            }
        }
    }

    public int getEdgeCount() {
        return edgeCount;
    }

    public void addMetricRegistry(MetricRegistry metricRegistry) {
    }
}
