package cn.fudan.cs.stree.query;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.*;

public abstract class Automata<L> {

    // suffix language containment matrix for conflict detection
    protected boolean containmentMark[][];

    // store source-target state pairs for each label
    // A label can contain multiple state transitions
    protected HashMap<L, HashMap<Integer, Integer>> labelTransitions;

    // for mapping states to contigious range from [0..n-1]
    private int stateCounter;
    private Map<Object, Integer> stateNumberMapping;

    // set of final states
    public Set<Integer> finalStates;

    protected Automata() {
        labelTransitions = Maps.newHashMap();
        stateCounter = 0;
        stateNumberMapping = Maps.newHashMap();
        finalStates = Sets.newHashSet();
    }

    /**
     * Finalize function must be called before query can be used for processing
     * It performs optimizations that are provided by specific implementation such as determinization, minimization etc.
     * This method also maps all states to a contigious range, and generates mappings for states and transitions
     * Finally, it computes the containment relation to be used for conlfict detection
     */
    public abstract void finalize();

    public int getNumOfStates() {
        // after finalization, state counter will point the number of states
        return stateCounter;
    }

    public Set<L> getAlphabet() {
        return this.labelTransitions.keySet();
    }


    public Map<Integer, Integer> getTransition(L label) {
        return labelTransitions.getOrDefault(label, Maps.newHashMap());
    }

    public void addFinalState(int state) {
        finalStates.add(state);
    }

    public boolean isFinalState(int state) {
        return finalStates.contains(state);
    }

    /**
     * Assign each state to a consecutive range in [0..n-1]
     * @param state
     * @return
     */
    protected int getStateNumber(Object state) {
        if(stateNumberMapping.containsKey(state)) {
            return stateNumberMapping.get(state);
        } else {
            int stateNumber = stateCounter++;
            stateNumberMapping.put(state, stateNumber);
            return  stateNumber;
        }
    }

    public boolean hasContainment(Integer stateQ, Integer stateT) {
        if(stateQ == null) {
            return true;
        }
        return !this.containmentMark[stateQ][stateT];
    }

    //计算关系转换之间是否存在包含的关系
    protected void computeContainmentRelationship() {
        int alphabetSize = getAlphabet().size();
        int numOfStates = getNumOfStates();

        // initialize the containmentMark matrix
        this.containmentMark = new boolean[numOfStates][numOfStates];

        // once we construct the minimized DFA, we can easily compute the sufflix language containment relationship
        // Algorithm S of Wood'95
        ArrayList[][] statePairMatrix = new ArrayList[numOfStates][numOfStates];
        for(int s = 0; s < numOfStates; s++) {
            for (int t = 0; t < numOfStates; t++) {
                statePairMatrix[s][t] = new ArrayList<>();
            }
        }
        // first create a transition matrix for the DFA
        // What state does the stored state become through label (alphabet)?
        int[][] transitionMatrix = new int[numOfStates][alphabetSize];
        for(int i = 0; i < numOfStates; i++) {
            for(int j = 0; j < alphabetSize; j++) {
                transitionMatrix[i][j] = -1;
            }
        }

        // Here, the hashmap of the two states of the already established label mapping is used to create a corresponding transformation matrix.
        Iterator<L> edgeIterator = getAlphabet().iterator();
        for(int j = 0 ; j < alphabetSize; j++) {
            Map<Integer, Integer> edges = getTransition(edgeIterator.next());
            for (Map.Entry<Integer, Integer> edge : edges.entrySet()) {
                transitionMatrix[edge.getKey ()][j] = edge.getValue();
            }
        }

        // initialize: line 1 of Algorithm S
        for(int s = 0; s < numOfStates; s++) {
            for (int t = 0; t < numOfStates; t++) {
                // for s \in S-F and t \in F
                if(!isFinalState(s) && isFinalState(t)) {
                    containmentMark[s][t] = true;
                }
            }
        }

        // line 2-7 of Algorithm S0
        for(int s = 0; s < numOfStates; s++) {
            for (int t = 0; t < numOfStates; t++) {
                // for s,t \in ((SxS) - ((S-F)xF))
                if(isFinalState(s) || !isFinalState(t)) {
                    // implement line 3,
                    boolean isMarked = false;
                    Queue<StatePair> markQueue = new ArrayDeque<>();
                    for(int j = 0; j < alphabetSize; j++) {
                        if(transitionMatrix[s][j] == transitionMatrix[t][j] && transitionMatrix[s][j] != -1) {
                            isMarked = true;
                            markQueue.add(StatePair.createInstance(s,t));
                        }
                    }

                    // recursively mark all the pairs on the list of pairs that are marked in this step
                    // line 5 of the Algorithm S
                    while(!markQueue.isEmpty()) {
                        StatePair pair = markQueue.poll();
                        List<StatePair> pairList = statePairMatrix[pair.stateS][pair.stateT];
                        for(StatePair candidate : pairList) {
                            if(!containmentMark[candidate.stateS][candidate.stateT]) {
                                markQueue.add(candidate);
                                containmentMark[candidate.stateS][candidate.stateT] = true;
                            }
                        }
                    }

                    // if there is no marked, then populate the lists
                    // line 6 of Algorithm S
                    if(!isMarked) {
                        for(int j = 0; j < alphabetSize; j++) {
                            int sEndpoint = transitionMatrix[s][j];
                            int tEndpoint = transitionMatrix[t][j];
                            if(sEndpoint != -1 && tEndpoint != -1 && sEndpoint != tEndpoint) {
                                // Line 7 of Algorithm S
                                statePairMatrix[sEndpoint][tEndpoint].add(StatePair.createInstance(s,t));
                            }
                        }
                    }
                }
            }
        }
    }

    private static class StatePair {
        public final int stateS, stateT;

        private StatePair(int stateS, int  stateT) {
            this.stateS = stateS;
            this.stateT = stateT;
        }

        public static StatePair createInstance(int stateS, int stateT) {
            return new StatePair(stateS,stateT);
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) return true;
            if (!(o instanceof StatePair)) {
                return false;
            }

            StatePair sp = (StatePair) o;
            return this.stateS == sp.stateS && this.stateT == sp.stateT;
        }

        @Override
        public int hashCode() {
            int result = 17;
            result = 31 * result + this.stateS;
            result = 31 * result + this.stateT;
            return result;
        }

        @Override
        public String toString() {
            return new StringBuilder().
                    append("<").append(stateS).append(",").append(stateT)
                    .append(">").toString();
        }
    }
}
