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
package com.hazelcast.simulator.common;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public enum FailureType {

    NETTY_EXCEPTION("nettyException", "Netty exception", false),
    WORKER_EXCEPTION("workerException", "Worker exception", false),
    WORKER_TIMEOUT("workerTimeout", "Worker timeout", false),
    WORKER_OOME("workerOOME", "Worker OOME", true),
    WORKER_ABNORMAL_EXIT("workerAbnormalExit", "Worker abnormal exit", true),
    WORKER_NORMAL_EXIT("workerNormalExit", "Worker normal exit", true),
    WORKER_CREATE_ERROR("workerCreateError", "Worker create error", true);

    private final String id;
    private final String humanReadable;
    private final boolean isTerminal;

    FailureType(String id, String humanReadable, boolean isTerminal) {
        this.id = id;
        this.humanReadable = humanReadable;
        this.isTerminal = isTerminal;
    }

    public String getId() {
        return id;
    }

    @Override
    public String toString() {
        return humanReadable;
    }

    /**
     * Checks if the failure is a terminal failure for the worker. E.g. process has exited or OOME etc.
     *
     * @return true if terminal.
     */
    public boolean isTerminal() {
        return isTerminal;
    }

    public boolean isPoisonPill() {
        return this == WORKER_NORMAL_EXIT;
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
