package cn.fudan.cs.stree.engine;

import cn.fudan.cs.stree.data.AbstractTRD;
import cn.fudan.cs.stree.data.AbstractTRDNode;
import cn.fudan.cs.stree.data.ProductGraph;
import cn.fudan.cs.stree.data.ResultPair;
import cn.fudan.cs.stree.query.Automata;
import cn.fudan.cs.stree.util.Constants;

import java.util.HashMap;
import java.util.concurrent.Callable;

public abstract class AbstractTRDExpansionJob<L, T extends AbstractTRD<Integer, T, N>, N extends AbstractTRDNode<Integer, T, N>>
        implements Callable<HashMap<Integer, Long>> {

    protected ProductGraph<Integer,L> productGraph;
    protected Automata<L> automata;
    protected T[] spanningTree;
    protected N[] parentNode;
    protected int[] targetVertex;
    protected int[] targetState;
    protected long[] edgeTimestamp;
    protected long[] labeledTimestamp;
    protected int currentSize;
    protected int resultCount;
    protected long windowSize;
    protected T tree;
    protected ResultPair results;
    protected long step;
    protected boolean isDeletion;

//    protected int[] tuple_num;

    protected AbstractTRDExpansionJob(ProductGraph<Integer,L> productGraph, Automata<L> automata, ResultPair results, long windowsize, T tree, long step) {
        this.productGraph = productGraph;
        this.automata = automata;
        this.results = results;
        this.windowSize = windowsize;
        this.tree = tree;
        this.step = step;
    }

    protected AbstractTRDExpansionJob(ProductGraph<Integer,L> productGraph, Automata<L> automata, ResultPair results, long windowsize, long step) {
        this.productGraph = productGraph;
        this.automata = automata;
        this.targetVertex = new int[Constants.EXPECTED_BATCH_SIZE];
        this.targetState = new int[Constants.EXPECTED_BATCH_SIZE];
        this.edgeTimestamp = new long[Constants.EXPECTED_BATCH_SIZE];
        this.labeledTimestamp = new long[Constants.EXPECTED_BATCH_SIZE];
        this.results = results;
        this.currentSize = 0;
        this.resultCount = 0;
        this.windowSize = windowsize;
        this.step = step;
    }

    /**
     * Populates the job array
     * @param spanningTree
     * @param parentNode
     * @param targetVertex
     * @param targetState
     * @param edgeTimestamp
     * @return false whenever job array is full and cannot be further populated
     */
    public  boolean addJob(T spanningTree, N parentNode, int targetVertex, int targetState,
                           long edgeTimestamp, long labeledTimestamp) throws IllegalStateException {
        if(this.currentSize >= Constants.EXPECTED_BATCH_SIZE) {
            throw new IllegalStateException("Job capacity exceed limit " + currentSize);
        }
        this.labeledTimestamp[currentSize] = labeledTimestamp;
        this.spanningTree[currentSize] = spanningTree;
        this.parentNode[currentSize] = parentNode;
        this.targetVertex[currentSize] = targetVertex;
        this.targetState[currentSize] = targetState;
        this.edgeTimestamp[currentSize] = edgeTimestamp;
        this.currentSize++;

        if(currentSize == Constants.EXPECTED_BATCH_SIZE - 1) {
            return false;
        }

        return true;
    }

    public  boolean addJob(T spanningTree, N parentNode, int targetVertex, int targetState,
                           long edgeTimestamp, long labeledTimestamp, int tuple_num) throws IllegalStateException {
        if(this.currentSize >= Constants.EXPECTED_BATCH_SIZE) {
            throw new IllegalStateException("Job capacity exceed limit " + currentSize);
        }
        this.labeledTimestamp[currentSize] = labeledTimestamp;
        this.spanningTree[currentSize] = spanningTree;
        this.parentNode[currentSize] = parentNode;
        this.targetVertex[currentSize] = targetVertex;
        this.targetState[currentSize] = targetState;
        this.edgeTimestamp[currentSize] = edgeTimestamp;
        this.currentSize++;

        if(currentSize == Constants.EXPECTED_BATCH_SIZE - 1) {
            return false;
        }

        return true;
    }

    public boolean addCWVJob(T spanningTree){
        if(this.currentSize >= Constants.EXPECTED_BATCH_SIZE) {
            throw new IllegalStateException("Job capacity exceed limit " + currentSize);
        }
        this.spanningTree[currentSize] = spanningTree;
        this.currentSize++;

        if(currentSize == Constants.EXPECTED_BATCH_SIZE - 1) {
            return false;
        }

        return true;
    }

    /**
     * Determines whether the current batch is full
     * @return
     */
    public boolean isFull() {
        return currentSize == Constants.EXPECTED_BATCH_SIZE - 1;
    }

    /**
     * Determines whether the current batch is empty
     * @return
     */
    public boolean isEmpty() {
        return currentSize == 0;
    }

    @Override
    public abstract HashMap<Integer, Long> call() throws Exception;
}
