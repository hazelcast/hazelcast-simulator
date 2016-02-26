package com.hazelcast.simulator.protocol.registry;

/**
 * Defines a target type for a Worker to select groups of Workers from the {@link ComponentRegistry}.
 *
 * The type {@link #CLIENT} selects all kinds of client Workers (Java, C#, C++, Python etc.).
 */
public enum TargetType {

    /**
     * Selects all types of Workers.
     */
    ALL,

    /**
     * Selects just member Workers.
     */
    MEMBER,

    /**
     * Selects just client Workers.
     */
    CLIENT,

    /**
     * Selects client Workers if there are any registered, member Workers otherwise.
     *
     * This equates to the old passive members mode.
     */
    PREFER_CLIENT;

    public boolean isMemberTarget() {
        return (this == ALL || this == MEMBER);
    }

    public TargetType resolvePreferClients(boolean hasClientWorkers) {
        if (this != PREFER_CLIENT) {
            return this;
        }
        return (hasClientWorkers ? CLIENT : MEMBER);
    }

    public String toString(int targetTypeCount) {
        if (this == ALL) {
            if (targetTypeCount == 0) {
                return "all";
            }
            return "" + targetTypeCount;
        }
        if (targetTypeCount == 0) {
            return "all " + name().toLowerCase();
        }
        return targetTypeCount + " " + name().toLowerCase();
    }
}
