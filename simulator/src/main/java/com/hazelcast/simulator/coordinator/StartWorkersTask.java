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
package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.cluster.ClusterLayout;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.protocol.registry.WorkerData;

import static com.hazelcast.simulator.utils.CommonUtils.getElapsedSeconds;
import static com.hazelcast.simulator.utils.FormatUtils.HORIZONTAL_RULER;

public class StartWorkersTask {

    private final ClusterLayout clusterLayout;
    private final RemoteClient remoteClient;
    private final ComponentRegistry componentRegistry;
    private final Echoer echoer;

    public StartWorkersTask(
            ClusterLayout clusterLayout,
            RemoteClient remoteClient,
            ComponentRegistry componentRegistry) {
        this.clusterLayout = clusterLayout;
        this.remoteClient = remoteClient;
        this.componentRegistry = componentRegistry;
        this.echoer = new Echoer(remoteClient);
    }

    public void run() {
        long started = System.nanoTime();

        echoer.echo(HORIZONTAL_RULER);
        echoer.echo("Starting Workers...");
        echoer.echo(HORIZONTAL_RULER);

        int totalWorkerCount = clusterLayout.getTotalWorkerCount();
        echoer.echo("Starting %d Workers (%d members, %d clients)...",
                totalWorkerCount,
                clusterLayout.getMemberWorkerCount(),
                clusterLayout.getClientWorkerCount());
        remoteClient.createWorkers(clusterLayout, true);

        if (componentRegistry.workerCount() > 0) {
            WorkerData firstWorker = componentRegistry.getFirstWorker();
            echoer.echo("Worker for global test phases will be %s (%s)", firstWorker.getAddress(),
                    firstWorker.getSettings().getWorkerType());
        }

        long elapsed = getElapsedSeconds(started);
        echoer.echo(HORIZONTAL_RULER);
        echoer.echo("Finished starting of %s Worker JVMs (%s seconds)", totalWorkerCount, elapsed);
        echoer.echo(HORIZONTAL_RULER);
    }
}
