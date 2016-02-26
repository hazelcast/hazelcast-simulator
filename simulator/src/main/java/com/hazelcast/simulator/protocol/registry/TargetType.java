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
package com.hazelcast.simulator.protocol.registry;

/**
 * Defines a target type for a Worker to select groups of Workers from the {@link ComponentRegistry}.
 *
 * The type {@link #CLIENT} selects all kinds of client Workers (Java, C#, C++, Python etc.).
 */
public enum TargetType {

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
    CLIENT,

    /**
     * Selects client Workers if there are any registered, member Workers otherwise.
     *
     * This equates to the old passive members mode.
     */
    PREFER_CLIENT;

    public boolean isMemberTarget() {
        return (this == ALL || this == MEMBER);
    }

    public TargetType resolvePreferClients(boolean hasClientWorkers) {
        if (this != PREFER_CLIENT) {
            return this;
        }
        return (hasClientWorkers ? CLIENT : MEMBER);
    }

    public String toString(int targetTypeCount) {
        if (this == ALL) {
            if (targetTypeCount == 0) {
                return "all";
            }
            return "" + targetTypeCount;
        }
        if (targetTypeCount == 0) {
            return "all " + name().toLowerCase();
        }
        return targetTypeCount + " " + name().toLowerCase();
    }
}
