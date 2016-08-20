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
package com.hazelcast.simulator.worker;

/**
 * Defines the different types for Simulator Worker components.
 */
public enum WorkerType {

    MEMBER(true, "member"),
    CLIENT(false, "client");

    private final boolean isMember;
    private final String id;

    WorkerType(boolean isMember, String id) {
        this.isMember = isMember;
        this.id = id;
    }

    public String id() {
        return id;
    }

    public static WorkerType getById(String id) {
        for (WorkerType workerType : values()) {
            if (workerType.id.equals(id)) {
                return workerType;
            }
        }
        return null;
    }

    public boolean isMember() {
        return isMember;
    }

    public String toLowerCase() {
        return name().toLowerCase();
    }
}
