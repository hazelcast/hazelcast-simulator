/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.protocol.operation;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;

/**
 * Counts invocations per {@link OperationType} for sent and received operations.
 */
public final class OperationTypeCounter {

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
        StringBuilder sb = new StringBuilder("Operation statistics (sent/received):");
        sb.append(NEW_LINE);
        long totalSent = 0;
        long totalReceived = 0;
        for (OperationType operationType : OperationType.values()) {
            long sent = SENT_OPERATIONS.get(operationType).get();
            long received = RECEIVED_OPERATIONS.get(operationType).get();
            sb.append(operationType).append(": ").append(sent).append('/').append(received).append(NEW_LINE);
            totalSent += sent;
            totalReceived += received;
        }
        sb.append("TOTAL: ").append(totalSent).append('/').append(totalReceived);
        LOGGER.log(level, sb.toString());
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
