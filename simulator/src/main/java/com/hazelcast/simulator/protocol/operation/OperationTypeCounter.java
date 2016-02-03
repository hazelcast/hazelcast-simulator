package com.hazelcast.simulator.protocol.operation;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;

/**
 * Counts invocations per {@link OperationType} for sent and received operations.
 */
public class OperationTypeCounter {

    private static final Logger LOGGER = Logger.getLogger(OperationTypeCounter.class);

    private static final Map<OperationType, AtomicLong> SENT_OPERATIONS = new ConcurrentHashMap<OperationType, AtomicLong>();
    private static final Map<OperationType, AtomicLong> RECEIVED_OPERATIONS = new ConcurrentHashMap<OperationType, AtomicLong>();

    static {
        reset();
    }

    private OperationTypeCounter() {
    }

    public static void sent(OperationType operationType) {
        SENT_OPERATIONS.get(operationType).incrementAndGet();
    }

    public static void received(OperationType operationType) {
        RECEIVED_OPERATIONS.get(operationType).incrementAndGet();
    }

    public static void printStatistics() {
        printStatistics(Level.DEBUG);
    }

    public static void printStatistics(Level level) {
        LOGGER.log(level, "Operation statistics (sent/received):");
        long totalSent = 0;
        long totalReceived = 0;
        for (OperationType operationType : OperationType.values()) {
            long sent = SENT_OPERATIONS.get(operationType).get();
            long received = RECEIVED_OPERATIONS.get(operationType).get();
            LOGGER.log(level, format("%s: %d/%d", operationType, sent, received));
            totalSent += sent;
            totalReceived += received;
        }
        LOGGER.log(level, format("TOTAL: %d/%d", totalSent, totalReceived));
    }

    // just for testing
    static long getSent(OperationType operationType) {
        return SENT_OPERATIONS.get(operationType).get();
    }

    // just for testing
    static long getReceived(OperationType operationType) {
        return RECEIVED_OPERATIONS.get(operationType).get();
    }

    static void reset() {
        for (OperationType operationType : OperationType.values()) {
            SENT_OPERATIONS.put(operationType, new AtomicLong());
            RECEIVED_OPERATIONS.put(operationType, new AtomicLong());
        }
    }
}
