package cn.fudan.cs.stree.data.arbitrary;

import cn.fudan.cs.stree.data.AbstractTRD;
import cn.fudan.cs.stree.data.Delta;
import cn.fudan.cs.stree.util.Hasher;

import java.util.LinkedList;

public class TRDRAPQ<V> extends AbstractTRD<V, TRDRAPQ<V>, TRDNodeRAPQ<V>> {

    protected TRDRAPQ(Delta<V, TRDRAPQ<V>, TRDNodeRAPQ<V>> delta, V vertex, int state, V root,
                      long edgeTimestamp) {
        super(delta, edgeTimestamp);
        LinkedList<Long> upper = new LinkedList<>();
        upper.add(Long.MAX_VALUE);
        LinkedList<Long> lower = new LinkedList<>();
        lower.add((long) 0);
        /* This point must be the root point */
        this.rootNode = new TRDNodeRAPQ<V>(vertex, state, lower, upper);
        this.root_vertex = root;
        this.rootNode.setAttribute_root(root);
        nodeIndex.put(Hasher.createTreeNodePairKey(vertex, state), rootNode);
    }
}
