package cn.fudan.cs.stree.data.arbitrary;

import cn.fudan.cs.stree.data.AbstractTRDNode;
import cn.fudan.cs.stree.util.Hasher;

import java.io.Serializable;
import java.util.LinkedList;

public class TRDNodeRAPQ<V> extends AbstractTRDNode<V, TRDRAPQ<V>, TRDNodeRAPQ<V>> implements Serializable {

    private int hash = 0;


    public TRDNodeRAPQ(V vertex, int state, LinkedList<Long> lower_bound_timestamp, LinkedList<Long> upper_bound_timestamp) {
        super(vertex, state, lower_bound_timestamp, upper_bound_timestamp);
    }

    public TRDNodeRAPQ(V vertex, int state) {
        super(vertex, state, new LinkedList<>(), new LinkedList<>());
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof TRDNodeRAPQ)) {
            return false;
        }

        TRDNodeRAPQ tuple = (TRDNodeRAPQ) o;

        return tuple.vertex.equals(vertex) && tuple.state == state;
    }

    @Override
    public int hashCode() {
        int h = hash;
        if(h == 0) {
            if (attribute_root == null)
                h = Hasher.TreeNodeHasher(vertex.hashCode(), state, 0);
            else {
                h = 31 * h + vertex.hashCode();
                h = 31 * h + state;
                h = 31 * h + attribute_root.hashCode();
            }
            hash = h;
        }
        return h;
    }

    @Override
    public String toString() {
        return "Node <" + getVertex() + "," + getState() + "," + getAttribute_root() + "> labeled ts = " + getLabeledTimestamp()
                + " [lower " + lower_bound_timestamp.toString()  + "] [upper " + upper_bound_timestamp.toString() + "]";
    }

}
