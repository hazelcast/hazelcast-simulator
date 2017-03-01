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

import com.hazelcast.simulator.common.RunMode;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import org.apache.log4j.Logger;

import static com.hazelcast.simulator.utils.CloudProviderUtils.isEC2;
import static com.hazelcast.simulator.utils.CloudProviderUtils.runMode;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.HarakiriMonitorUtils.getStartHarakiriMonitorCommandOrNull;
import static com.hazelcast.simulator.utils.NativeUtils.execute;
import static java.lang.String.format;

public final class AgentUtils {

    private static final String SIMULATOR_VERSION = getSimulatorVersion();

    private AgentUtils() {
    }

    public static void checkInstallation(Bash bash, SimulatorProperties properties, ComponentRegistry registry) {
        if (runMode(properties) != RunMode.Remote) {
            return;
        }

        ThreadSpawner spawner = new ThreadSpawner("checkInstallation", true);
        for (AgentData agentData : registry.getAgents()) {
            spawner.spawn(new CheckAgentInstallationTask(agentData, bash));
        }
        spawner.awaitCompletion();
    }

    public static void startAgents(Logger logger, Bash bash, SimulatorProperties properties, ComponentRegistry registry) {
        logger.info(format("Starting %d Agents...", registry.agentCount()));
        ThreadSpawner spawner = new ThreadSpawner("startAgents", true);
        int agentPort = properties.getAgentPort();
        for (AgentData agentData : registry.getAgents()) {
            spawner.spawn(new StartAgentTask(logger, bash, properties, agentData, agentPort));
        }
        spawner.awaitCompletion();
        logger.info(format("Successfully started %d Agents", registry.agentCount()));
    }

    public static void stopAgents(Logger logger, Bash bash, SimulatorProperties properties, ComponentRegistry registry) {
        String startHarakiriMonitorCommand = getStartHarakiriMonitorCommandOrNull(properties);

        logger.info(format("Stopping %d Agents...", registry.agentCount()));
        ThreadSpawner spawner = new ThreadSpawner("stopAgents", true);
        for (AgentData agentData : registry.getAgents()) {
            spawner.spawn(new StopAgentTask(logger, bash, properties, agentData, startHarakiriMonitorCommand));
        }
        spawner.awaitCompletion();
        logger.info(format("Successfully stopped %d Agents", registry.agentCount()));
    }

    private static class CheckAgentInstallationTask implements Runnable {

        private final AgentData agentData;
        private final Bash bash;

        CheckAgentInstallationTask(AgentData agentData, Bash bash) {
            this.agentData = agentData;
            this.bash = bash;
        }

        @Override
        public void run() {
            String ip = agentData.getPublicAddress();
            String result;
            try {
                result = bash.ssh(ip, format("[[ -f hazelcast-simulator-%s/bin/agent ]] && echo SIM-OK || echo SIM-NOK",
                        SIMULATOR_VERSION), true, false).trim();
            } catch (CommandLineExitException e) {
                throw new CommandLineExitException(format(
                        "Could not connect to %s. Please check your agents.txt file for invalid IP addresses.%n%s",
                        ip, e.getCause().getMessage()));
            }
            if (result.endsWith("SIM-NOK")) {
                throw new CommandLineExitException(format(
                        "Simulator is not installed correctly on %s. Please run provisioner --install to fix this.", ip));
            }
        }
    }

    private static final class StartAgentTask implements Runnable {

        private final Logger logger;
        private final Bash bash;

        private final String ip;
        private final String mandatoryParameters;
        private final String optionalParameters;
        private final String ec2Parameters;
        private final RunMode runMode;

        private StartAgentTask(Logger logger, Bash bash, SimulatorProperties properties, AgentData agentData, int agentPort) {
            this.logger = logger;
            this.bash = bash;
            this.runMode = CloudProviderUtils.runMode(properties);

            this.ip = agentData.getPublicAddress();
            this.mandatoryParameters = format("--addressIndex %d --publicAddress %s --port %s",
                    agentData.getAddressIndex(), ip, agentPort);
            this.optionalParameters = format(" --threadPoolSize %d --workerLastSeenTimeoutSeconds %d",
                    properties.getAgentThreadPoolSize(),
                    properties.getWorkerLastSeenTimeoutSeconds());

            if (isEC2(properties)) {
                this.ec2Parameters = format(" --cloudProvider %s --cloudIdentity %s --cloudCredential %s",
                        properties.getCloudProvider(),
                        properties.getCloudIdentity(),
                        properties.getCloudCredential());
            } else {
                this.ec2Parameters = "";
            }
        }

        @Override
        public void run() {
            switch (runMode) {
                case Remote:
                    runRemote();
                    break;
                case Embedded:
                    // do nothing.
                    break;
                case Local:
                    runLocal();
                    break;
                default:
                    throw new IllegalStateException("Unknown runMode: " + runMode);
            }
        }

        private void runLocal() {
            logger.info(format("Starting Agent on %s", ip));

            execute(format("nohup %s/bin/agent %s%s > agent.out 2> agent.err < /dev/null &",
                    getSimulatorHome(), mandatoryParameters, optionalParameters));

            execute(format("%s/bin/.await-file-exists agent.pid", getSimulatorHome()));
        }

        private void runRemote() {
            logger.info(format("Killing Java processes on %s", ip));
            bash.killAllJavaProcesses(ip, false);

            logger.info(format("Starting Agent on %s", ip));
            bash.ssh(ip, format("nohup hazelcast-simulator-%s/bin/agent %s%s%s > agent.out 2> agent.err < /dev/null &",
                    SIMULATOR_VERSION, mandatoryParameters, optionalParameters, ec2Parameters));

            bash.ssh(ip, format("hazelcast-simulator-%s/bin/.await-file-exists agent.pid", SIMULATOR_VERSION));
        }
    }

    private static final class StopAgentTask implements Runnable {

        private final Logger logger;
        private final Bash bash;
        private final RunMode runMode;

        private final String ip;
        private final String startHarakiriMonitorCommand;

        private StopAgentTask(Logger logger, Bash bash, SimulatorProperties properties, AgentData agentData,
                              String startHarakiriMonitorCommand) {
            this.logger = logger;
            this.bash = bash;
            this.runMode = CloudProviderUtils.runMode(properties);

            this.ip = agentData.getPublicAddress();
            this.startHarakiriMonitorCommand = startHarakiriMonitorCommand;
        }

        @Override
        public void run() {
            switch (runMode) {
                case Remote:
                    runRemote();
                    break;
                case Embedded:
                    // do nothing.
                    break;
                case Local:
                    runLocal();
                    break;
                default:
                    throw new IllegalStateException("Unknown runMode: " + runMode);
            }
        }

        private void runLocal() {
            logger.info(format("Stopping Agent %s", ip));
            bash.execute(format("%s/bin/.kill-from-pid-file agent.pid", getSimulatorHome()));
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
