package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.simulator.protocol.exception.ExceptionLogger;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

class TestExceptionLogger implements ExceptionLogger {

    private static final Logger LOGGER = Logger.getLogger(TestExceptionLogger.class);

    private final List<ExceptionEntry> exceptionEntries = new ArrayList<ExceptionEntry>();

    @Override
    public long getLogInvocationCount() {
        return exceptionEntries.size();
    }

    @Override
    public void log(Throwable cause) {
        log(cause, null);
    }

    @Override
    public void log(Throwable cause, String testId) {
        if (cause == null) {
            LOGGER.fatal("Cannot call log with a null exception");
            return;
        }

        LOGGER.warn("Exception detected.", cause);
        ExceptionEntry exceptionEntry = new ExceptionEntry(cause, testId);

        exceptionEntries.add(exceptionEntry);
    }

    void assertNoException() {
        assertEquals(0, exceptionEntries.size());
    }

    void assertException(Class<?>... exceptionTypes) {
        assertEquals(format("Expected %d exceptions, but found %d", exceptionTypes.length, exceptionEntries.size()),
                exceptionTypes.length, exceptionEntries.size());

        Iterator<ExceptionEntry> iterator = exceptionEntries.iterator();
        for (Class<?> exceptionType : exceptionTypes) {
            ExceptionEntry exceptionEntry = iterator.next();

            Throwable throwable = exceptionEntry.cause;
            String throwableClassName = throwable.getClass().getSimpleName();
            throwable.printStackTrace();
            assertEquals(format("Expected %s, but was %s for test %s: %s", exceptionType.getSimpleName(), throwableClassName,
                    exceptionEntry.testId, throwable.getMessage()), exceptionType, throwable.getClass());
        }
    }

    private static class ExceptionEntry {

        private final Throwable cause;
        private final String testId;

        private ExceptionEntry(Throwable cause, String testId) {
            this.cause = cause;
            this.testId = testId;
        }
    }
}
