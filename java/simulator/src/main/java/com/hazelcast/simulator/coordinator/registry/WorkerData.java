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

import com.hazelcast.simulator.agent.workerprocess.WorkerParameters;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Contains the metadata of a Simulator Worker.
 * <p>
 * Part of the metadata is the {@link SimulatorAddress} which is used by the Simulator Communication Protocol.
 */
public class WorkerData {

    private final SimulatorAddress address;
    private final WorkerParameters parameters;
    private final Map<String, String> tags;
    private volatile boolean ignoreFailures;

    WorkerData(WorkerParameters parameters) {
        this(parameters, new HashMap<>());
    }

    WorkerData(WorkerParameters parameters, Map<String, String> tags) {
        this.address = SimulatorAddress.fromString(parameters.get("WORKER_ADDRESS"));
        this.parameters = parameters;
        this.tags = tags;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public SimulatorAddress getAddress() {
        return address;
    }

    public WorkerParameters getParameters() {
        return parameters;
    }

    public boolean isMemberWorker() {
        return parameters.getWorkerType().equals("member");
    }

    public boolean isIgnoreFailures() {
        return ignoreFailures;
    }

    public void setIgnoreFailures(boolean ignoreFailures) {
        this.ignoreFailures = ignoreFailures;
    }

    @Override
    public String toString() {
        return "WorkerData{address=" + address + '}';
    }

    public static String toAddressString(Collection<WorkerData> workers) {
        StringBuilder sb = new StringBuilder();

        boolean first = true;
        for (WorkerData worker : workers) {
            if (first) {
                first = false;
            } else {
                sb.append(",");
            }
            sb.append(worker.getAddress());
        }

        return sb.toString();
    }
}
