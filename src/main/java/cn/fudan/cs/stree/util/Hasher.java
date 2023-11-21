package cn.fudan.cs.stree.util;

import java.io.Serializable;

public class Hasher {
    private static ThreadLocal<MapKey> threadLocalKey = new ThreadLocal<>();
    private static ThreadLocal<MapKey_With_Root> threadLocalKey_WithRoot = new ThreadLocal<>();
    public static int TreeNodeHasher(int vertex, int state, int root) {
        int h = 17;
        h = 31 * h + vertex;
        h = 31 * h + state;
        h = 31 * h + root;
        return h;
    }

    public static <V> MapKey<V> createTreeNodePairKey(V vertex, int state) {
        MapKey<V> mapKey = new MapKey<V>(vertex, state);
        return mapKey;
    }

    public static <V> MapKey_With_Root<V> createTreeNodePairKeyWithRoot(V vertex, int state, V root) {
        MapKey_With_Root<V> mapKey = new MapKey_With_Root<V>(vertex, state, root);
        return mapKey;
    }

    public static <V> MapKey<V> getThreadLocalTreeNodePairKey(V vertex, int state) {

        MapKey<V> mapKey = threadLocalKey.get();
        if(mapKey == null) {
            mapKey = new MapKey<V>(vertex, state);
            threadLocalKey.set(mapKey);
        } else {
            mapKey.X = vertex;
            mapKey.Y = state;
        }
        return mapKey;
    }

    public static class MapKey<V> implements Serializable {

        public V X;
        public int Y;

        public MapKey(V X, int Y) {
            this.X = X;
            this.Y = Y;
        }

        @Override
        public boolean equals (final Object O) {
            if (!(O instanceof MapKey)) return false;
            if (!((MapKey) O).X.equals(X)) return false;
            if (((MapKey) O).Y != Y) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int h = 17;
            h = 31 * h + X.hashCode();
            h = 31 * h + Y;
            return h;
        }
        @Override
        public String toString() {
            return "<" + X + ", " + Y + ">";
        }
    }

    public static <V> MapKey_With_Root<V> getThreadLocalTreeNodePairKey_With_Root(V vertex, int state, V root) {
        MapKey_With_Root<V> mapKey = threadLocalKey_WithRoot.get();
        if(mapKey == null) {
            mapKey = new MapKey_With_Root<V>(vertex, state, root);
            threadLocalKey_WithRoot.set(mapKey);
        } else {
            mapKey.X = vertex;
            mapKey.Y = state;
            mapKey.Z = root;
        }
        return mapKey;
//        MapKey_With_Root<V> mapKey = new MapKey_With_Root<V>(vertex, state, root);
//        return mapKey;
    }

    public static class MapKey_With_Root<V> {

        public V X;
        public int Y;
        public V Z;

        public MapKey_With_Root(V X, int Y, V Z) {
            this.X = X;
            this.Y = Y;
            this.Z = Z;
        }

        @Override
        public boolean equals (final Object O) {
            if (!(O instanceof MapKey_With_Root)) return false;
            if (!((MapKey_With_Root) O).X.equals(X)) return false;
            if (((MapKey_With_Root) O).Y != Y) return false;
            if (!((MapKey_With_Root) O).Z.equals(Z)) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int h = 17;
            h = 31 * h + X.hashCode();
            h = 31 * h + Y;
            h = 31 * h + Z.hashCode();
            return h;
        }

        @Override
        public String toString() {
            return "<" + X + ", " + Y + ", " + Z +">";
        }
    }
}
