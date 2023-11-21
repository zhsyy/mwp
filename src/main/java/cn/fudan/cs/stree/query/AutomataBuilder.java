package cn.fudan.cs.stree.query;

public interface AutomataBuilder<A, C> {
    A transition(C label);

    A kleeneStar(A nfa);

    A concenetation(A first, A second);

    A alternation(A first, A second);
}
