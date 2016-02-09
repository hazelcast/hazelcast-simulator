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
package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import org.apache.log4j.Logger;

import java.io.File;

import static com.hazelcast.simulator.utils.CloudProviderUtils.isEC2;
import static com.hazelcast.simulator.utils.CloudProviderUtils.isLocal;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.HarakiriMonitorUtils.getStartHarakiriMonitorCommandOrNull;
import static com.hazelcast.simulator.utils.NativeUtils.execute;
import static java.lang.String.format;

public final class AgentUtils {

    private static final File SIMULATOR_HOME = getSimulatorHome();
    private static final String SIMULATOR_VERSION = getSimulatorVersion();

    private AgentUtils() {
    }

    public static void startAgents(Logger logger, Bash bash, SimulatorProperties simulatorProperties,
                                   ComponentRegistry componentRegistry) {
        logger.info(format("Starting %d Agents...", componentRegistry.agentCount()));
        ThreadSpawner spawner = new ThreadSpawner("startAgents", true);
        int agentPort = simulatorProperties.getAgentPort();
        for (AgentData agentData : componentRegistry.getAgents()) {
            spawner.spawn(new StartRunnable(logger, bash, simulatorProperties, agentData, agentPort));
        }
        spawner.awaitCompletion();
        logger.info(format("Successfully started %d Agents", componentRegistry.agentCount()));
    }

    public static void stopAgents(Logger logger, Bash bash, SimulatorProperties simulatorProperties,
                                  ComponentRegistry componentRegistry) {
        String startHarakiriMonitorCommand = getStartHarakiriMonitorCommandOrNull(simulatorProperties);

        logger.info(format("Stopping %d Agents...", componentRegistry.agentCount()));
        ThreadSpawner spawner = new ThreadSpawner("stopAgents", true);
        for (AgentData agentData : componentRegistry.getAgents()) {
            spawner.spawn(new StopRunnable(logger, bash, simulatorProperties, agentData, startHarakiriMonitorCommand));
        }
        spawner.awaitCompletion();
        logger.info(format("Successfully stopped %d Agents", componentRegistry.agentCount()));
    }

    private static final class StartRunnable implements Runnable {

        private final Logger logger;
        private final Bash bash;
        private final boolean isLocal;

        private final String ip;
        private final String mandatoryParameters;
        private final String optionalParameters;
        private final String ec2Parameters;

        private StartRunnable(Logger logger, Bash bash, SimulatorProperties simulatorProperties, AgentData agentData,
                              int agentPort) {
            this.logger = logger;
            this.bash = bash;
            this.isLocal = isLocal(simulatorProperties);

            this.ip = agentData.getPublicAddress();
            this.mandatoryParameters = format("--addressIndex %d --publicAddress %s --port %s",
                    agentData.getAddressIndex(), ip, agentPort);
            this.optionalParameters = format(" --threadPoolSize %d --workerLastSeenTimeoutSeconds %d",
                    simulatorProperties.getAgentThreadPoolSize(),
                    simulatorProperties.getWorkerLastSeenTimeoutSeconds());
            if (isEC2(simulatorProperties)) {
                this.ec2Parameters = format(" --cloudProvider %s --cloudIdentity %s --cloudCredential %s",
                        simulatorProperties.getCloudProvider(),
                        simulatorProperties.getCloudIdentity(),
                        simulatorProperties.getCloudCredential());
            } else {
                this.ec2Parameters = "";
            }
        }

        @Override
        public void run() {
            if (isLocal) {
                runLocal();
            } else {
                runRemote();
            }
        }

        private void runLocal() {
            logger.info(format("Starting Agent on %s", ip));
            execute(format("nohup %s/bin/agent %s%s > agent.out 2> agent.err < /dev/null &",
                    SIMULATOR_HOME, mandatoryParameters, optionalParameters));

            execute(format("%s/bin/.await-file-exists agent.pid", SIMULATOR_HOME));
        }

        private void runRemote() {
            logger.info(format("Killing Java processes on %s", ip));
            bash.killAllJavaProcesses(ip);

            logger.info(format("Starting Agent on %s", ip));
            bash.ssh(ip, format("nohup hazelcast-simulator-%s/bin/agent %s%s%s > agent.out 2> agent.err < /dev/null &",
                    SIMULATOR_VERSION, mandatoryParameters, optionalParameters, ec2Parameters));

            bash.ssh(ip, format("hazelcast-simulator-%s/bin/.await-file-exists agent.pid", SIMULATOR_VERSION));
        }
    }

    private static final class StopRunnable implements Runnable {

        private final Logger logger;
        private final Bash bash;
        private final boolean isLocal;

        private final String ip;
        private final String startHarakiriMonitorCommand;

        private StopRunnable(Logger logger, Bash bash, SimulatorProperties simulatorProperties, AgentData agentData,
                             String startHarakiriMonitorCommand) {
            this.logger = logger;
            this.bash = bash;
            this.isLocal = isLocal(simulatorProperties);

            this.ip = agentData.getPublicAddress();
            this.startHarakiriMonitorCommand = startHarakiriMonitorCommand;
        }

        @Override
        public void run() {
            if (isLocal) {
                runLocal();
            } else {
                runRemote();
            }
        }

        private void runLocal() {
            logger.info(format("Stopping Agent %s", ip));
            execute(format("%s/bin/.kill-from-pid-file agent.pid", SIMULATOR_HOME));
        }

        private void runRemote() {
            logger.info(format("Stopping Agent %s", ip));
            bash.ssh(ip, format("hazelcast-simulator-%s/bin/.kill-from-pid-file agent.pid", SIMULATOR_VERSION));

            if (startHarakiriMonitorCommand != null) {
                logger.info(format("Starting HarakiriMonitor on %s", ip));
                bash.ssh(ip, startHarakiriMonitorCommand);
            }
        }
    }
}
