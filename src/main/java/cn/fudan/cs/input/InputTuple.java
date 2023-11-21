package cn.fudan.cs.input;

import java.io.Serializable;

public class InputTuple<S,T,L> implements Serializable {

    private S source;
    private T target;
    private L label;
    private long timestamp;

    private boolean ifDeletion;

    /**
     * @{@link Type}, INSERT by default
     * @param source
     * @param target
     * @param label
     */
    public InputTuple(S source, T target, L label) {
        this(source, target, label, 0);
    }

    public InputTuple(S source, T target, L label, long timestamp) {
        this.source = source;
        this.target = target;
        this.label = label;
        this.timestamp = timestamp;
    }

    public S getSource() {
        return source;
    }

    public T getTarget() {
        return target;
    }

    public L getLabel() {
        return label;
    }

    public void setSource(S source) {
        this.source = source;
    }

    public void setTarget(T target) {
        this.target = target;
    }

    public void setLabel(L label) {
        this.label = label;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public int hashCode() {
        int h = 17;
        h = 31 * h + source.hashCode();
        h = 31 * h + label.hashCode();
        h = 31 * h + target.hashCode();
        return h;
    }

    @Override
    public String toString() {
        return "<" + this.source + "," + this.target + "," + this.label + "> ts : " + this.timestamp;
    }

    public boolean isDeletion() {
        return ifDeletion;
    }

    public void setIfDeletion(boolean ifDeletion) {
        this.ifDeletion = ifDeletion;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
