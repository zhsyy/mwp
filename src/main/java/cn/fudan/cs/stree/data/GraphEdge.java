package cn.fudan.cs.stree.data;

public class GraphEdge<V> {

    private int h = 0;

    private V source;
    private V target;
    private long timestamp;

    public GraphEdge(V source, V target, long timestamp) {
        this.source = source;
        this.target = target;
        this.timestamp = timestamp;
    }

    public V getSource() {
        return source;
    }

    public V getTarget() {
        return target;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GraphEdge<?> graphEdge = (GraphEdge<?>) o;

        if (!source.equals(graphEdge.source)) return false;
        return (target.equals(graphEdge.target));
    }

    @Override
    public int hashCode() {
        if(h == 0) {
            int result = source.hashCode();
            result = 31 * result + target.hashCode();
            h = result;
        }
        return h;
    }

    @Override
    public String toString() {
        return "GraphEdge{" +
                ", source=" + source +
                ", target=" + target +
                ", timestamp=" + timestamp +
                '}';
    }
}
