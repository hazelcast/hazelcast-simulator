package com.hazelcast.simulator.test;

import static java.lang.String.format;

/**
 * Exception for failures in Simulator tests.
 *
 * Should be used instead of a {@link RuntimeException}.
 */
public class TestException extends RuntimeException {

    public TestException(Throwable cause) {
        super(cause);
    }

    public TestException(String message) {
        super(message);
    }

    public TestException(String message, Object... args) {
        super(format(message, args));

        Object lastArg = args[args.length - 1];
        if (lastArg instanceof Throwable) {
            initCause((Throwable) lastArg);
        }
    }
}
