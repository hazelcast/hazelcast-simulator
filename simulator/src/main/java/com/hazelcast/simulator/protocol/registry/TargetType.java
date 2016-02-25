package com.hazelcast.simulator.protocol.registry;

/**
 * Defines a target type for a Worker to select groups of Workers from the {@link ComponentRegistry}.
 *
 * The type {@link #CLIENT} selects all kinds of client Workers (Java, C#, C++, Python etc.).
 */
public enum TargetType {

    /**
     * Returns all types of Workers.
     */
    ALL,

    /**
     * Returns just member Workers.
     */
    MEMBER,

    /**
     * Returns just client Workers.
     */
    CLIENT
}
