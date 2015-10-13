package com.hazelcast.simulator.test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public enum FailureType {

    NETTY_EXCEPTION("nettyException", "Netty exception", false),

    WORKER_EXCEPTION("workerException", "Worker exception", false),
    WORKER_TIMEOUT("workerTimeout", "Worker timeout", true),
    WORKER_OOM("workerOOM", "Worker OOME", true),
    WORKER_EXIT("workerExit", "Worker exit failure", true),
    WORKER_FINISHED("workerFinished", "Worker finished", true);

    private final String id;
    private final String humanReadable;
    private final boolean isWorkerFinishedFailure;

    FailureType(String id, String humanReadable, boolean isWorkerFinishedFailure) {
        this.id = id;
        this.humanReadable = humanReadable;
        this.isWorkerFinishedFailure = isWorkerFinishedFailure;
    }

    public String getId() {
        return id;
    }

    @Override
    public String toString() {
        return humanReadable;
    }

    public boolean isWorkerFinishedFailure() {
        return isWorkerFinishedFailure;
    }

    public boolean isPoisonPill() {
        return (this == WORKER_FINISHED);
    }

    public static Set<FailureType> fromPropertyValue(String propertyValue) {
        if (propertyValue == null || propertyValue.isEmpty()) {
            return Collections.emptySet();
        }
        Set<FailureType> result = new HashSet<FailureType>();
        StringTokenizer tokenizer = new StringTokenizer(propertyValue, ",");
        while (tokenizer.hasMoreTokens()) {
            String id = tokenizer.nextToken().trim();
            FailureType failureType = getById(id);
            result.add(failureType);
        }
        return result;
    }

    public static String getIdsAsString() {
        FailureType[] types = FailureType.values();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < types.length; i++) {
            builder.append(types[i].id);
            if (i < types.length - 1) {
                builder.append(", ");
            }
        }
        return builder.toString();
    }

    private static FailureType getById(String id) {
        for (FailureType type : FailureType.values()) {
            if (type.id.equals(id)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown failure ID: " + id);
    }
}
