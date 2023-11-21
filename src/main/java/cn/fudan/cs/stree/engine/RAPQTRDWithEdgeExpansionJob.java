package cn.fudan.cs.stree.engine;

import cn.fudan.cs.stree.data.*;
import cn.fudan.cs.stree.data.arbitrary.TRDNodeRAPQ;
import cn.fudan.cs.stree.data.arbitrary.TRDRAPQ;
import cn.fudan.cs.stree.util.Constants;
import cn.fudan.cs.stree.query.Automata;

import java.util.*;

public class RAPQTRDWithEdgeExpansionJob<L> extends AbstractTRDExpansionJob<L, TRDRAPQ<Integer>, TRDNodeRAPQ<Integer>> {
    public RAPQTRDWithEdgeExpansionJob(ProductGraph<Integer,L> productGraph, Automata<L> automata, ResultPair results,
                                       long windowsize, long step, boolean isDeletion) {
        super(productGraph, automata, results, windowsize, step);
        this.isDeletion = isDeletion;
        this.spanningTree = new TRDRAPQ[Constants.EXPECTED_BATCH_SIZE];
        this.parentNode = new TRDNodeRAPQ[Constants.EXPECTED_BATCH_SIZE];
    }

    @Override
    public HashMap<Integer, Long> call() {
        // call each job in teh buffer
        if(isDeletion) {
            for (int i = 0; i < currentSize; i++) {
                processDeletion(spanningTree[i], targetVertex[i], targetState[i]);
            }
        } else {
            for (int i = 0; i < currentSize; i++) {
                processTransition(spanningTree[i], parentNode[i], targetVertex[i], targetState[i], edgeTimestamp[i]);
            }
        }
        return null;
    }

    public void processTransition(TRDRAPQ<Integer> tree, TRDNodeRAPQ<Integer> parentNode,
                                  int childVertex, int childState, long edgeTimestamp) {

        LinkedList<TRDNodeRAPQ<Integer>> parentNodes = new LinkedList<>();
        parentNodes.add(parentNode);

        LinkedList<Integer> childVertexList = new LinkedList<>();
        childVertexList.add(childVertex);

        LinkedList<Integer> childStateList = new LinkedList<>();
        childStateList.add(childState);

        LinkedList<Long> edgeTimestampList = new LinkedList<>();
        edgeTimestampList.add(edgeTimestamp);

        while (!childVertexList.isEmpty()){
            boolean modify = false;
            TRDNodeRAPQ<Integer> childNode;
            childVertex = childVertexList.removeFirst();
            childState = childStateList.removeFirst();
            edgeTimestamp = edgeTimestampList.removeFirst();
            parentNode = parentNodes.removeFirst();
            if(tree.exists(childVertex, childState)) {
                // if the child node already exists, we might need to update timestamp
                childNode = tree.getNodes(childVertex, childState);
                modify = childNode.calculateAndUpdateTimestamps(edgeTimestamp, windowSize, step, parentNode);
            } else {
                childNode = tree.addNode(parentNode, childVertex, childState, edgeTimestamp, windowSize, step);
                if (childNode != null) {
                    modify = true;
                }
            }

            if (modify) {
                if (automata.isFinalState(childState) && tree.getRootVertex() != childVertex) {
                    // send new result to a final machine instead of recording locally
                    results.addNewResult(tree.getRootVertex(), childVertex, childNode);
                }

                if (childNode.checkIfUpper(tree)){}
                else if (childNode.checkIfLower(tree)){}

                Collection<GraphEdge<ProductGraphNode<Integer>>> forwardEdges = productGraph.getForwardEdges(childVertex, childState);
                if (forwardEdges != null) {
                    // there are forward edges, iterate over them
                    for (GraphEdge<ProductGraphNode<Integer>> forwardEdge : forwardEdges) {
                        if (childNode.checkIfMerge(forwardEdge.getTimestamp(), windowSize))
                            continue;
                        // recursive call as the target of the forwardEdge has not been visited in state targetState before
                        childVertexList.add(forwardEdge.getTarget().getVertex());
                        childStateList.add(forwardEdge.getTarget().getState());
                        edgeTimestampList.add(forwardEdge.getTimestamp());
                        parentNodes.add(childNode);
                    }
                }
            }
        }
    }

    public void processDeletion(TRDRAPQ<Integer> tree, int childVertex, int childState){
        if(tree.exists(childVertex, childState)) {
            Queue<TRDNodeRAPQ<Integer>> queue = new ArrayDeque<>();
            queue.add(tree.getNodes(childVertex, childState));
            while (!queue.isEmpty()){
                TRDNodeRAPQ<Integer> node = queue.poll();
                Collection<GraphEdge<ProductGraphNode<Integer>>> backwardEdges = productGraph.getBackwardEdges(childVertex, childState);
                LinkedList<AbstractTRDNode> parentNodesList = new LinkedList<>();
                LinkedList<Long> edgeTimestampList = new LinkedList<>();
                if (backwardEdges != null){
                    for (GraphEdge<ProductGraphNode<Integer>> backwardEdge : backwardEdges) {
                        ProductGraphNode<Integer> productGraphNode = backwardEdge.getSource();
                        if (tree.exists(productGraphNode.getVertex(), productGraphNode.getState())) {
                            edgeTimestampList.add(backwardEdge.getTimestamp());
                            parentNodesList.add(tree.getNodes(productGraphNode.getVertex(), productGraphNode.getState()));
                        }
                    }
                }

                if (!node.recalculateTs(parentNodesList, edgeTimestampList, windowSize, step, tree)){
                    Collection<GraphEdge<ProductGraphNode<Integer>>> forwardEdges = productGraph.getForwardEdges(childVertex, childState);
                    if (forwardEdges != null){
                        for (GraphEdge<ProductGraphNode<Integer>> forwardEdge : forwardEdges) {
                            ProductGraphNode<Integer> productGraphNode = forwardEdge.getTarget();
                            if (tree.exists(productGraphNode.getVertex(), productGraphNode.getState())) {
                                queue.add(tree.getNodes(productGraphNode.getVertex(), productGraphNode.getState()));
                            }
                        }
                    }
                }
            }
        }
    }
}
