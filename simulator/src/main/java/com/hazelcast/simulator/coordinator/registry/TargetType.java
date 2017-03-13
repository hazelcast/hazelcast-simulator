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
package com.hazelcast.simulator.coordinator.registry;

/**
 * Defines a target type for a Worker to select groups of Workers from the {@link ComponentRegistry}.
 *
 * The type {@link #PREFER_CLIENT} equates to the old passive member mode.
 * The type {@link #CLIENT} selects all kinds of client Workers (Java, C#, C++, Python etc.).
 */
public enum TargetType {

    /**
     * Selects client Workers if there are any registered, member Workers otherwise.
     *
     * Will be resolved to {@link #MEMBER} or {@link #CLIENT} before usage (see {@link #resolvePreferClient(boolean)}).
     */
    PREFER_CLIENT,

    /**
     * Selects all types of Workers.
     */
    ALL,

    /**
     * Selects just member Workers.
     */
    MEMBER,

    /**
     * Selects just client Workers.
     */
    CLIENT;

    public TargetType resolvePreferClient(boolean hasClientWorkers) {
        if (this != PREFER_CLIENT) {
            return this;
        }
        return (hasClientWorkers ? CLIENT : MEMBER);
    }

    public boolean matches(boolean isMemberWorker) {
        return (this == ALL || (this == MEMBER && isMemberWorker) || (this == CLIENT && !isMemberWorker));
    }

    public String toString(int targetCount) {
        if (this == ALL) {
            if (targetCount > 0) {
                return targetCount + " " + (targetCount == 1 ? "Worker" : "Workers");
            }
            return "all Workers";
        }
        if (targetCount > 0) {
            return targetCount + " " + name().toLowerCase() + " " + (targetCount == 1 ? "Worker" : "Workers");
        }
        return "all " + name().toLowerCase() + " Workers";
    }

    public static String getIdsAsString() {
        StringBuilder builder = new StringBuilder();
        String delimiter = "";
        for (TargetType targetType : values()) {
            builder.append(delimiter).append(targetType);
            delimiter = ", ";
        }
        return builder.toString();
    }
}
