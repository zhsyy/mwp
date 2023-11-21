package cn.fudan.cs.stree.data;

import cn.fudan.cs.stree.engine.AbstractTRDExpansionJob;
import cn.fudan.cs.stree.query.Automata;

public interface  ObjectFactory<V, T extends AbstractTRD<V, T, N>, N extends AbstractTRDNode<V, T, N>> {

    N createTRDNode(T tree, V vertex, int state);

    T createTRD(Delta<V, T, N> delta, V vertex, int state, V root, long edgeTimestamp);

    <L> AbstractTRDExpansionJob createExpansionJob(ProductGraph<Integer,L> productGraph, Automata<L> automata, ResultPair results,
                                                   long windowsize, long step, boolean isDeletion);

//    <L> AbstractTRDExpansionJob createCWVExpansionJob(ProductGraph<Integer,L> productGraph, Automata<L> automata, Set<ResultPair<Integer>> results,
//                                                       long windowsize, T tree, int size, long step);
}
