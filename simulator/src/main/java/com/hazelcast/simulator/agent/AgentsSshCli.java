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
package com.hazelcast.simulator.agent;

import com.hazelcast.simulator.common.AgentsFile;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.coordinator.registry.AgentData;
import com.hazelcast.simulator.coordinator.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.BashCommand;
import com.hazelcast.simulator.utils.CliUtils;
import com.hazelcast.simulator.utils.CommandLineExitException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.List;

import static com.hazelcast.simulator.common.GitInfo.getBuildTime;
import static com.hazelcast.simulator.common.GitInfo.getCommitIdAbbrev;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.FileUtils.getConfigurationFile;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.FormatUtils.join;
import static com.hazelcast.simulator.utils.SimulatorUtils.loadComponentRegister;
import static com.hazelcast.simulator.utils.SimulatorUtils.loadSimulatorProperties;
import static java.lang.String.format;

public final class AgentsSshCli {

    private static final Logger LOGGER = Logger.getLogger(AgentsSshCli.class);

    private final OptionParser parser = new OptionParser();

    private final OptionSpec testSpec = parser.accepts("test",
            "Checks if ssh connection to the agents can be made.");
    private final File agentsFile = new File(getUserDir(), AgentsFile.NAME);

    private final OptionSet options;
    private final SimulatorProperties properties;
    private final ComponentRegistry registry;

    private AgentsSshCli(String[] args) {
        options = CliUtils.initOptionsWithHelp(parser, args);
        this.properties = loadSimulatorProperties();
        this.registry = loadComponentRegister(agentsFile, false);

        if (options.has(testSpec)) {
            new BashCommand(getConfigurationFile("agent_ssh_check.sh").getAbsolutePath())
                    .addParams(join(registry.getAgentIps(), ","))
                    .addEnvironment(properties.asMap())
                    .setSystemOut(true)
                    .execute();
        } else {
            List commands = options.nonOptionArguments();
            if (commands.size() != 1) {
                throw new CommandLineExitException("1 argument expected");
            }
            String command = (String) commands.get(0);
            String sshOptions = properties.get("SSH_OPTIONS");
            String simulatorUser = properties.get("SIMULATOR_USER");

            for (AgentData agent : registry.getAgents()) {
                System.out.println("[" + agent.getPublicAddress() + "]");
                new BashCommand("ssh -n -o LogLevel=quiet " + sshOptions + " " + simulatorUser
                        + "@" + agent.getPublicAddress() + " '" + command + "'")
                        .setSystemOut(true)
                        .addEnvironment(properties.asMap())
                        .execute();
            }
        }
    }

    public static void main(String[] args) {
        LOGGER.info("Hazelcast Simulator Agent-SSH");
        LOGGER.info(format("Version: %s, Commit: %s, Build Time: %s",
                getSimulatorVersion(), getCommitIdAbbrev(), getBuildTime()));
        LOGGER.info(format("SIMULATOR_HOME: %s", getSimulatorHome().getAbsolutePath()));

        try {
            new AgentsSshCli(args);
        } catch (Exception e) {
            exitWithError(LOGGER, "Could not start Agent!", e);
        }
    }
}
