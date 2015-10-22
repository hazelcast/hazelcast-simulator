/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.simulator.agent.workerjvm;

import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.utils.ThreadSpawner;
import io.netty.buffer.ByteBuf;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.simulator.protocol.core.SimulatorMessageCodec.getSourceAddress;

public class WorkerJvmManager {

    private static final Logger LOGGER = Logger.getLogger(WorkerJvmManager.class);

    private final ConcurrentMap<SimulatorAddress, WorkerJvm> workerJVMs = new ConcurrentHashMap<SimulatorAddress, WorkerJvm>();

    public void add(SimulatorAddress workerAddress, WorkerJvm workerJvm) {
        workerJVMs.put(workerAddress, workerJvm);
    }

    public Collection<WorkerJvm> getWorkerJVMs() {
        return workerJVMs.values();
    }

    public void updateLastSeenTimestamp(ByteBuf buffer) {
        SimulatorAddress sourceAddress = getSourceAddress(buffer);
        AddressLevel sourceAddressLevel = sourceAddress.getAddressLevel();
        if (sourceAddressLevel == AddressLevel.WORKER) {
            updateLastSeenTimestamp(sourceAddress);
        } else if (sourceAddressLevel == AddressLevel.TEST) {
            updateLastSeenTimestamp(sourceAddress.getParent());
        }
    }

    public void updateLastSeenTimestamp(Response response) {
        for (Map.Entry<SimulatorAddress, ResponseType> responseTypeEntry : response.entrySet()) {
            updateLastSeenTimestamp(responseTypeEntry.getKey());
        }
    }

    private void updateLastSeenTimestamp(SimulatorAddress sourceAddress) {
        WorkerJvm workerJvm = workerJVMs.get(sourceAddress);
        if (workerJvm != null) {
            workerJvm.updateLastSeen();
        }
    }

    public void shutdown() {
        ThreadSpawner spawner = new ThreadSpawner("workerJvmManagerShutdown", true);
        for (final WorkerJvm workerJvm : new ArrayList<WorkerJvm>(workerJVMs.values())) {
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    shutdown(workerJvm);
                }
            });
        }
        spawner.awaitCompletion();
    }

    public void shutdown(WorkerJvm workerJvm) {
        workerJVMs.remove(workerJvm.getAddress());
        try {
            // this sends SIGTERM on *nix
            workerJvm.getProcess().destroy();
            workerJvm.getProcess().waitFor();
        } catch (Exception e) {
            LOGGER.error("Failed to destroy worker process: " + workerJvm, e);
        }
    }
}
