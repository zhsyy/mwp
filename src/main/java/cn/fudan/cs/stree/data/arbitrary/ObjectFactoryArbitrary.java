package cn.fudan.cs.stree.data.arbitrary;

import cn.fudan.cs.stree.data.ProductGraph;
import cn.fudan.cs.stree.engine.AbstractTRDExpansionJob;
import cn.fudan.cs.stree.data.Delta;
import cn.fudan.cs.stree.data.ObjectFactory;
import cn.fudan.cs.stree.data.ResultPair;
import cn.fudan.cs.stree.engine.RAPQTRDWithEdgeExpansionJob;
import cn.fudan.cs.stree.query.Automata;

public class ObjectFactoryArbitrary<V> implements ObjectFactory<V, TRDRAPQ<V>, TRDNodeRAPQ<V>> {
    @Override
    public TRDNodeRAPQ<V> createTRDNode(TRDRAPQ<V> tree, V vertex, int state) {
        return new TRDNodeRAPQ<V>(vertex, state);
    }

    @Override
    public TRDRAPQ<V> createTRD(Delta<V, TRDRAPQ<V>, TRDNodeRAPQ<V>> delta, V vertex, int state, V root,
                                long edgeTimestamp) {
        return new TRDRAPQ<V>(delta, vertex, state, root, edgeTimestamp);
    }

    @Override
    public <L> AbstractTRDExpansionJob createExpansionJob(ProductGraph<Integer, L> productGraph, Automata<L> automata, ResultPair results,
                                                          long windowsize, long step, boolean isDeletion) {
        return new RAPQTRDWithEdgeExpansionJob<>(productGraph, automata, results, windowsize, step, isDeletion);
    }
}
