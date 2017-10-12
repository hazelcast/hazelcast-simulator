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
package com.hazelcast.simulator.coordinator.tasks;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
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
import static com.hazelcast.simulator.utils.FileUtils.rename;
import static com.hazelcast.simulator.utils.NativeUtils.execute;
import static java.lang.String.format;

public class ArtifactDownloadTask {

    private static final Logger LOGGER = Logger.getLogger(ArtifactDownloadTask.class);
    private static final String RSYNC_COMMAND = "rsync --copy-links %s-avv -e \"ssh %s\" %s@%%s:%%s %s";

    private final String sessionId;
    private final SimulatorProperties simulatorProperties;
    private final File outputDirectory;
    private final ComponentRegistry componentRegistry;
    private final Bash bash;
    private final String sshOptions;
    private final String sshUser;

    public ArtifactDownloadTask(String sessionId,
                                SimulatorProperties simulatorProperties,
                                File outputDirectory,
                                ComponentRegistry componentRegistry) {
        this.sessionId = sessionId;
        this.simulatorProperties = simulatorProperties;
        this.outputDirectory = outputDirectory;
        this.componentRegistry = componentRegistry;
        this.bash = new Bash(simulatorProperties);
        this.sshOptions = simulatorProperties.getSshOptions();
        this.sshUser = simulatorProperties.getUser();
    }

    public void run() {
        if (isLocal(simulatorProperties)) {
            downloadLocal();
        } else {
            downloadRemote();
        }
    }

    private void downloadLocal() {
        LOGGER.info("Retrieving artifacts of local machine");

        File workerHome = newFile(getSimulatorHome(), WORKERS_HOME_NAME);
        String workerPath = workerHome.getAbsolutePath();

        if ("*".equals(sessionId)) {
            execute(format("cp -r %s/* %s || true", workerPath, outputDirectory.getAbsolutePath()));
        } else {
            execute(format("cp -r %s/%s/* %s || true", workerPath, sessionId, outputDirectory.getAbsolutePath()));
        }

        execute(format("mv ./agent.err %s/ || true", outputDirectory.getAbsolutePath()));
        execute(format("mv ./agent.out %s/ || true", outputDirectory.getAbsolutePath()));
    }

    private void downloadRemote() {
        long started = System.nanoTime();
        LOGGER.info(format("Download artifacts of %s machines...", componentRegistry.agentCount()));

        ThreadSpawner spawner = new ThreadSpawner("download", true);

        for (AgentData agentData : componentRegistry.getAgents()) {
            spawner.spawn(new DownloadWorkerLogs(agentData.getPublicAddress()));
        }

        spawner.spawn(new DownloadAgentLogs());

        spawner.awaitCompletion();

        long elapsed = getElapsedSeconds(started);
        LOGGER.info(format("Finished downloading artifacts of %s machines (%s seconds)",
                componentRegistry.agentCount(), elapsed));
    }

    private final class DownloadWorkerLogs implements Runnable {
        private final String ip;

        private DownloadWorkerLogs(String ip) {
            this.ip = ip;
        }

        @Override
        public void run() {
            String workersPath = format("hazelcast-simulator-%s/workers/%s", getSimulatorVersion(), sessionId);

            String targetDirectory = "*".equals(sessionId)
                    ? outputDirectory.getAbsolutePath()
                    : outputDirectory.getParentFile().getAbsolutePath();

            String rsyncCommand = format(RSYNC_COMMAND, "", sshOptions, sshUser, targetDirectory);

            LOGGER.info(format("Downloading Worker logs from %s", ip));
            bash.executeQuiet(format(rsyncCommand, ip, workersPath));
        }
    }

    private final class DownloadAgentLogs implements Runnable {
        @Override
        public void run() {
            for (AgentData agentData : componentRegistry.getAgents()) {
                download(agentData);
            }
        }

        private void download(AgentData agentData) {
            String ip = agentData.getPublicAddress();
            String agentAddress = agentData.getAddress().toString();

            LOGGER.info(format("Downloading Agent logs from %s", ip));
            String outputPath = outputDirectory.getAbsolutePath();
            String rsyncCommand = format(RSYNC_COMMAND, "--backup --suffix=-%s ", sshOptions, sshUser, outputPath);

            bash.executeQuiet(format(rsyncCommand, ip, ip, "agent.out"));
            bash.executeQuiet(format(rsyncCommand, ip, ip, "agent.err"));

            File agentOut = new File(outputPath, "agent.out");
            File agentErr = new File(outputPath, "agent.err");

            rename(agentOut, new File(outputPath, agentAddress + '-' + ip + "-agent.out"));
            rename(agentErr, new File(outputPath, agentAddress + '-' + ip + "-agent.err"));
        }
    }
}
