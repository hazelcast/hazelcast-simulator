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
package com.hazelcast.simulator.protocol.exception;

public enum ExceptionType {

    COORDINATOR_EXCEPTION("Coordinator ran into an unhandled exception"),
    AGENT_EXCEPTION("Agent ran into an unhandled exception"),
    WORKER_EXCEPTION("Worked ran into an unhandled exception"),

    WORKER_TIMEOUT("Worker has not contacted Agent for a too long period"),
    WORKER_OOM("Worker ran into an OOME"),
    WORKER_EXIT("Worker terminated with a non-zero exit code");

    private final String humanReadable;

    ExceptionType(String humanReadable) {
        this.humanReadable = humanReadable;
    }

    public String getHumanReadable() {
        return humanReadable;
    }
}
