package cn.fudan.cs.stree.query;

public class NFAAutomataBuilder implements AutomataBuilder<NFA, String> {

    public NFAAutomataBuilder() {

    }

    /**
     * Create an NFA with a single transition with given label
     * @param label
     * @return
     */
    @Override
    public NFA transition(String label) {
        NFA nfa = new NFA();
        nfa.getEntry().addTransition(label, nfa.getExit());
        nfa.getExit().setFinal(true);

        return nfa;
    }

    /**
     * Create an NFA with a Kleene star over a given NFA
     * @param nfa
     * @return
     */
    @Override
    public NFA kleeneStar(NFA nfa) {
        nfa.getEntry().addEpsilonTransitions(nfa.getExit());
        nfa.getExit().addEpsilonTransitions(nfa.getEntry());

        return nfa;
    }

    /**
     * Creates a concenatation NFA from given two NFAs
     * @param first
     * @param second
     * @return
     */
    @Override
    public NFA concenetation(NFA first, NFA second) {
        first.getExit().setFinal(false);
        first.getExit().addEpsilonTransitions(second.getEntry());

        return new NFA(first.getEntry(), second.getExit());
    }

    /**
     * Creates a concenatation NFA from given two NFAs
     * @param first
     * @param second
     * @return
     */
    @Override
    public NFA alternation(NFA first, NFA second) {
        first.getExit().setFinal(false);
        second.getExit().setFinal(false);

        NFA newNFA = new NFA();

        newNFA.getEntry().addEpsilonTransitions(first.getEntry());
        newNFA.getEntry().addEpsilonTransitions(second.getEntry());

        first.getExit().addEpsilonTransitions(newNFA.getExit());
        second.getExit().addEpsilonTransitions(newNFA.getExit());

        newNFA.getExit().setFinal(true);

        return newNFA;
    }
}
