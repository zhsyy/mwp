package cn.fudan.cs.stree.query;

import cn.fudan.cs.stree.util.Constants;
import com.google.common.collect.*;

import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

public class NFA extends Automata<String> {

    private State<String> entry;
    private State<String> exit;

    private boolean isFinalized;

    public NFA() {
        super();
        entry = new State<>();
        exit = new State<>();
        isFinalized = false;
    }

    public NFA(State<String> entry, State<String>  exit) {
        this();
        this.entry = entry;
        this.exit = exit;
    }

    public State<String> getEntry() {
        return entry;
    }

    public State<String> getExit() {
        return exit;
    }

    /**
     * Checks whether this automata has been finalized. Finalized automatas cannot perform any further changes
     * @return
     */
    public boolean isFinalized() {
        return isFinalized;
    }

    /**
     * Minimize this automata and generate transition functions. This automata cannot be altered once it is finalizes.
     */
    public void finalize() {
        this.isFinalized = true;

        Set<State<String>> states = Sets.newHashSet();
        Set<String> alphabet = Sets.newHashSet();

        // data structure to store transitions
        labelTransitions.put(Constants.EPSILON_TRANSITION, Maps.newHashMap());

        //data structures for BFS traversal of the transition graph
        Set<State<String>> visited = Sets.newHashSet();
        Queue<State<String>> queue = Lists.newLinkedList();

        queue.offer(entry);

        while(!queue.isEmpty()) {
            State<String> state = queue.poll();
            visited.add(state);
            // mark if it is final state
            if(state.isFinal()) {
                addFinalState(getStateNumber(state));
            }

            // go over all outgoing edges with a labels
            for(String label : state.getTransitions().keySet()) {
                Collection<State> targetStates = state.getTransitions(label);
                Map<Integer, Integer> transitions = labelTransitions.computeIfAbsent(label, key -> Maps.newHashMap());

                for(State targetState : targetStates) {
                    transitions.put(getStateNumber(state), getStateNumber(targetState));
                    // add to queue if the state is not visited before
                    if(!visited.contains(targetState)) {
                        queue.offer(targetState);
                    }
                }
            }

            // go over all epsilon transitions
            Map<Integer, Integer> transitions = labelTransitions.get(Constants.EPSILON_TRANSITION);
            for(State targetState : state.getEmptyTransitions()) {
                transitions.put(getStateNumber(state), getStateNumber(targetState));
                // add to queue if the state is not visited before
                if(!visited.contains(targetState)) {
                    queue.offer(targetState);
                }
            }

        }

        // finally perform containment relationship computations
        computeContainmentRelationship();
    }

    public static class State<T> {

        private boolean isFinal;

        private Multimap<T, State> transitions;
        private Set<State> emptyTransitions;

        public State() {
            this(false);
        }

        public State(boolean isFinal) {
            this.isFinal = isFinal;
            transitions = HashMultimap.create();
            emptyTransitions = Sets.newHashSet();
        }

        public State(State<T> other, boolean isFinal) {
            this.isFinal = isFinal;
            transitions = HashMultimap.create(other.transitions);
            emptyTransitions = Sets.newHashSet(other.emptyTransitions);
        }

        public void addTransition(T label, State next) {
            transitions.put(label, next);
        }

        public void addEpsilonTransitions(State next) {
            emptyTransitions.add(next);
        }

        public boolean isFinal() {
            return isFinal;
        }

        public void setFinal(boolean aFinal) {
            isFinal = aFinal;
        }

        public Multimap<T, State> getTransitions() {
            return transitions;
        }

        public Collection<State> getTransitions(T label) {
            return transitions.get(label);
        }

        public Set<State> getEmptyTransitions() {
            return emptyTransitions;
        }
    }
}
