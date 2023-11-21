package cn.fudan.cs.stree.util;

import java.util.HashMap;
import java.util.Map;

public enum Semantics {

    SIMPLE ("simple"),
    ARBITRARY ("arbitrary");

    private final String semantics;

    private static final Map<String, Semantics> BY_LABEL = new HashMap<>();

    static {
        for(Semantics p : values()) {
            BY_LABEL.put(p.semantics, p);
        }
    }

    Semantics(String semantics) {
        this.semantics = semantics;
    }

    @Override
    public String toString() {
        return semantics;
    }

    public static Semantics fromValue(String semantics) {
        return BY_LABEL.get(semantics.toLowerCase());
    }
}
