package cn.fudan.cs.stree.data;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedDeque;

public class ProductGraphNode<V> {

    private V vertex;
    private int state;
    private ConcurrentLinkedDeque<GraphEdge<ProductGraphNode<V>>> forwardEdges;
    private ConcurrentLinkedDeque<GraphEdge<ProductGraphNode<V>>> backwardEdges;

    private int hash = 0;

    public ProductGraphNode(V vertex, int state) {
        this.vertex = vertex;
        this.state = state;

        this.forwardEdges = new ConcurrentLinkedDeque<>();
//                new ArrayDeque<>(Constants.EXPECTED_NEIGHBOURS);
        this.backwardEdges = new ConcurrentLinkedDeque<>();
    }

    public V getVertex() {
        return vertex;
    }

    public int getState() {
        return state;
    }

    protected void addForwardEdge(GraphEdge<ProductGraphNode<V>> forwardEdge) {
        this.forwardEdges.add(forwardEdge);
    }

    protected void addBackwardEdge(GraphEdge<ProductGraphNode<V>> backwardEdge) {
        this.backwardEdges.add(backwardEdge);
    }

    protected void removeForwardEdge(GraphEdge<ProductGraphNode<V>> forwardEdge) {
        this.forwardEdges.remove(forwardEdge);
    }

    protected void removeBackwardEdge(GraphEdge<ProductGraphNode<V>> backwardEdge) {
        this.backwardEdges.remove(backwardEdge);
    }

    public Collection<GraphEdge<ProductGraphNode<V>>> getForwardEdges() {
        return forwardEdges;
    }

    public Collection<GraphEdge<ProductGraphNode<V>>> getBackwardEdges() {
        return backwardEdges;
    }

    @Override

    public boolean equals(Object o) {
        if (o == this) return true;
        if (! (o instanceof ProductGraphNode)) return false;

        ProductGraphNode pair = (ProductGraphNode) o;

        return pair.vertex.equals(this.vertex) && pair.state == this.state;
    }

    // implementation from effective Java : Item 9
    @Override
    public int hashCode() {
        int h = hash;
        if (h == 0) {
            h = 17;
            h = 31 * h + state;
            h = 31 * h + vertex.hashCode();
            hash = h;
        }
        return h;
    }

    @Override
    public String toString() {
        return new StringBuilder("<").append(this.vertex)
                .append(",").append(this.state).append(">").toString();
    }
}
