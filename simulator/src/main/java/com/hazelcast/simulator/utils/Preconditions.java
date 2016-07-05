package com.hazelcast.simulator.utils;

public class Preconditions {

    /**
     * Tests if an argument is not null.
     *
     * @param argument     the argument tested to see if it is not null.
     * @param errorMessage the errorMessage
     * @return the argument that was tested.
     * @throws java.lang.NullPointerException if argument is null
     */
    public static <T> T checkNotNull(T argument, String errorMessage) {
        if (argument == null) {
            throw new NullPointerException(errorMessage);
        }
        return argument;
    }

    private Preconditions(){}
}
