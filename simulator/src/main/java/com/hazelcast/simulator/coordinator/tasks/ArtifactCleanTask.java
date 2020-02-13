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
package com.hazelcast.simulator.coordinator.tasks;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.coordinator.registry.AgentData;
import com.hazelcast.simulator.coordinator.registry.Registry;
import com.hazelcast.simulator.utils.Bash;
import com.hazelcast.simulator.utils.ThreadSpawner;
import org.apache.log4j.Logger;

import java.io.File;

import static com.hazelcast.simulator.agent.workerprocess.WorkerProcessLauncher.WORKERS_HOME_NAME;
import static com.hazelcast.simulator.utils.CloudProviderUtils.isLocal;
import static com.hazelcast.simulator.utils.CommonUtils.getElapsedSeconds;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.FileUtils.newFile;
import static com.hazelcast.simulator.utils.NativeUtils.execute;
import static java.lang.String.format;

public class ArtifactCleanTask {
    private static final Logger LOGGER = Logger.getLogger(ArtifactCleanTask.class);

    private final Registry registry;
    private final SimulatorProperties simulatorProperties;
    private final Bash bash;

    public ArtifactCleanTask(Registry registry, SimulatorProperties simulatorProperties) {
        this.registry = registry;
        this.simulatorProperties = simulatorProperties;
        this.bash = new Bash(simulatorProperties);
    }

    public void run() {
        if (isLocal(simulatorProperties)) {
            cleanLocal();
        } else {
            cleanRemote();
        }
    }

    private void cleanLocal() {
        File workerHome = newFile(getSimulatorHome(), WORKERS_HOME_NAME);
        LOGGER.info(format("Cleaning local worker directory %s ...", workerHome.getAbsolutePath()));

        String workerPath = workerHome.getAbsolutePath();
        execute(format("rm -fr %s || true", workerPath));

        LOGGER.info("Cleaning local worker directory complete!");
    }

    private void cleanRemote() {
        long started = System.nanoTime();

        LOGGER.info(format("Cleaning Worker homes of %s machines...", registry.agentCount()));

        final String cleanCommand = format("rm -fr hazelcast-simulator-%s/workers/*", getSimulatorVersion());

        ThreadSpawner spawner = new ThreadSpawner("clean", true);
        for (final AgentData agent : registry.getAgents()) {
            spawner.spawn(() -> {
                LOGGER.info(format("Cleaning %s", agent.getPublicAddress()));
                bash.ssh(agent.getPublicAddress(), cleanCommand);
            });
        }
        spawner.awaitCompletion();

        long elapsed = getElapsedSeconds(started);
        LOGGER.info(format("Finished cleaning Worker homes of %s machines (%s seconds)",
                registry.agentCount(), elapsed));
    }
}
