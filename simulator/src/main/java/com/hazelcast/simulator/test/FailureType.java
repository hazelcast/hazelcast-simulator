/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public enum FailureType {

    NETTY_EXCEPTION("nettyException", "Netty exception", false),

    WORKER_EXCEPTION("workerException", "Worker exception", false),
    WORKER_TIMEOUT("workerTimeout", "Worker timeout", false),
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
        StringBuilder builder = new StringBuilder();
        String delimiter = "";
        for (FailureType type : values()) {
            builder.append(delimiter).append(type.id);
            delimiter = ", ";
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
