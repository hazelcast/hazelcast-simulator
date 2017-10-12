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
package com.hazelcast.simulator.common;

import static com.hazelcast.simulator.utils.Preconditions.checkNotNull;

/**
 * Defines the different types for Simulator Worker components.
 */
public final class WorkerType {

    public static final WorkerType MEMBER = new WorkerType("member");
    public static final WorkerType JAVA_CLIENT = new WorkerType("javaclient");
    public static final WorkerType LITE_MEMBER = new WorkerType("litemember");

    private final String name;

    public WorkerType(String name) {
        this.name = checkNotNull(name, "name can't be null");
    }

    public String name() {
        return name;
    }

    public boolean isMember() {
        return MEMBER.name.equals(name);
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }

        if (o.getClass() != WorkerType.class) {
            return false;
        }

        WorkerType that = (WorkerType) o;
        return that.name.equals(this.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
}
