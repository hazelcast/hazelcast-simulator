package com.hazelcast.simulator.protocol.exception;

/**
 * Logs exceptions to files or Simulator Coordinator, depending on the implementation.
 */
public interface ExceptionLogger {

    /**
     * Maximum number of exceptions which will be logged.
     */
    int MAX_EXCEPTION_COUNT = 1000;

    /**
     * Returns the number of log method invocations.
     *
     * This value can be higher than {@value #MAX_EXCEPTION_COUNT}.
     *
     * @return the number of logged exceptions.
     */
    long getLogInvocations();

    /**
     * Logs an exception.
     *
     * @param cause the {@link Throwable} that should be logged.
     */
    void log(Throwable cause);

    /**
     * Logs an exception of a Simulator test.
     *
     * @param cause  the {@link Throwable} that should be logged.
     * @param testId the id of the test that caused the exception.
     */
    void log(Throwable cause, String testId);
}
