package cn.fudan.cs.stree.query;

import java.util.HashMap;

public class ManualQueryAutomata<L> extends Automata<L> {

    private HashMap<Integer, HashMap<L, Integer>> transitions;

    // overwrite the private field in super class
    private int numOfStates;

    public static ManualQueryAutomata<String> getManualQuery(String queryCase){
        ManualQueryAutomata<String> query = null;
        switch (queryCase){
            case "q_0":
                query = new ManualQueryAutomata<String>(2);
                query.addFinalState(1);
                query.addTransition(0, "a", 1);
                query.addTransition(1, "a", 1);
                break;
            case "q_1":
                query = new ManualQueryAutomata<String>(4);
                query.addFinalState(1);query.addFinalState(2);query.addFinalState(3);
                query.addTransition(0, "a", 1);
                query.addTransition(1, "b", 2);query.addTransition(1, "c", 3);
                query.addTransition(2, "b", 2);
                query.addTransition(2, "c", 3);
                query.addTransition(3, "c", 3);
                break;
            case "q_2":
                query = new ManualQueryAutomata<String>(4);
                query.addFinalState(3);
                query.addTransition(0, "a", 1);
                query.addTransition(1, "b", 2);
                query.addTransition(1, "c", 3);
                query.addTransition(2, "b", 2);
                query.addTransition(2, "c", 3);
                break;
            case "q_3":
                query = new ManualQueryAutomata<>(3);
                query.addFinalState(1);query.addFinalState(2);
                query.addTransition(0, "a", 1);
                query.addTransition(0, "b", 2);
                query.addTransition(1, "a", 1);
                query.addTransition(1, "b", 2);
                query.addTransition(2, "b", 2);
                break;
            case "q_4":
                query = new ManualQueryAutomata<String>(3);
                query.addFinalState(1);query.addFinalState(2);
                query.addTransition(0, "a", 1);
                query.addTransition(1, "b", 2);
                query.addTransition(2, "b", 2);
                break;
            case "q_5":
                query = new ManualQueryAutomata<String>(2);
                query.addFinalState(1);
                query.addTransition(0, "a", 1);
                query.addTransition(0, "b", 1);
                query.addTransition(1, "b", 1);
                break;
            case "q_6":
                query = new ManualQueryAutomata<String>(1);
                query.addFinalState(0);
                query.addTransition(0, "a", 0);
                query.addTransition(0, "b", 0);
                query.addTransition(0, "c", 0);
                break;
            case "q_7":
                query = new ManualQueryAutomata<String>(2);
                query.addFinalState(1);
                query.addTransition(0, "a", 1);
                query.addTransition(0, "b", 1);
                query.addTransition(1, "c", 1);
                break;
            default:
                break;
        }
        return query;
    }

    public ManualQueryAutomata(int numOfStates) {
        super();
        this.containmentMark = new boolean[numOfStates][numOfStates];
        transitions =  new HashMap<>();
        this.numOfStates = numOfStates;
        // initialize transition maps for all
        for(int i = 0; i < numOfStates; i++) {
            transitions.put(i, new HashMap<L, Integer>());
        }
    }

    public void addTransition(int source, L label, int target) {
        HashMap<L, Integer> forwardMap = transitions.get(source);
        getStateNumber(source);
        getStateNumber(target);
        forwardMap.put(label, target);
        if(!labelTransitions.containsKey(label)) {
            labelTransitions.put(label, new HashMap<>());
        }
        HashMap<Integer, Integer> labelMap = labelTransitions.get(label);
        labelMap.put(source, target);
    }

    @Override
    public void finalize() {
        // only thing to be performed is to compute the containment relationship
        computeContainmentRelationship();
    }
}
