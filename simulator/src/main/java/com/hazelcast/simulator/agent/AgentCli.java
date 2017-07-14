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

import com.hazelcast.simulator.utils.CliUtils;
import com.hazelcast.simulator.utils.CommandLineExitException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.log4j.Logger;

import static com.hazelcast.simulator.common.GitInfo.getBuildTime;
import static com.hazelcast.simulator.common.GitInfo.getCommitIdAbbrev;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static java.lang.String.format;

public final class AgentCli {
    private static final Logger LOGGER = Logger.getLogger(AgentCli.class);

    private static final int DEFAULT_WORKER_LAST_SEEN_TIMEOUT_SECONDS = 180;

    final Agent agent;

    private final OptionParser parser = new OptionParser();

    private final OptionSpec<Integer> addressIndexSpec = parser.accepts("addressIndex",
            "Address index of this Agent for the Simulator Communication Protocol.")
            .withRequiredArg().ofType(Integer.class);

    private final OptionSpec<String> publicAddressSpec = parser.accepts("publicAddress",
            "Public address of this Agent.")
            .withRequiredArg().ofType(String.class);

    private final OptionSpec<Integer> portSpec = parser.accepts("port",
            "Port of this Agent.")
            .withRequiredArg().ofType(Integer.class);

    private final OptionSpec<Integer> workerLastSeenTimeoutSecondsSpec = parser.accepts("workerLastSeenTimeoutSeconds",
            "Timeout value for worker timeout detection.")
            .withRequiredArg().ofType(Integer.class).defaultsTo(DEFAULT_WORKER_LAST_SEEN_TIMEOUT_SECONDS);

    private final OptionSpec<String> parentPidSpec = parser.accepts("parentPid",
            "The parentPid. Useful if the agent needs to terminate itself when the parent process has terminated. "
                    + "Only makes sense to be used for local instance.")
            .withRequiredArg().ofType(String.class);

    private final OptionSet options;

    AgentCli(String[] args) {
        logHeader();

        options = CliUtils.initOptionsWithHelp(parser, args);

        if (!options.has(addressIndexSpec)) {
            throw new CommandLineExitException("Missing parameter: --addressIndex");
        }
        int addressIndex = options.valueOf(addressIndexSpec);

        if (!options.has(publicAddressSpec)) {
            throw new CommandLineExitException("Missing parameter: --publicAddress");
        }
        String publicAddress = options.valueOf(publicAddressSpec);

        if (!options.has(portSpec)) {
            throw new CommandLineExitException("Missing parameter: --port");
        }
        int port = options.valueOf(portSpec);
        int workerLastSeenTimeoutSeconds = options.valueOf(workerLastSeenTimeoutSecondsSpec);

        String parentPid = options.valueOf(parentPidSpec);

        this.agent = new Agent(addressIndex, publicAddress, port, workerLastSeenTimeoutSeconds, parentPid);
    }

    private static void logHeader() {
        LOGGER.info("Hazelcast Simulator Agent");
        LOGGER.info(format("Version: %s, Commit: %s, Build Time: %s",
                getSimulatorVersion(), getCommitIdAbbrev(), getBuildTime()));
        LOGGER.info(format("SIMULATOR_HOME: %s%n", getSimulatorHome().getAbsolutePath()));

        logImportantSystemProperties();
    }

    private static void logImportantSystemProperties() {
        logSystemProperty("java.class.path");
        logSystemProperty("java.home");
        logSystemProperty("java.vendor");
        logSystemProperty("java.vendor.url");
        logSystemProperty("sun.java.command");
        logSystemProperty("java.version");
        logSystemProperty("os.arch");
        logSystemProperty("os.name");
        logSystemProperty("os.version");
        logSystemProperty("user.dir");
        logSystemProperty("user.home");
        logSystemProperty("user.name");
        logSystemProperty("SIMULATOR_HOME");
    }

    private static void logSystemProperty(String name) {
        LOGGER.info(format("%s=%s", name, System.getProperty(name)));
    }

    public static void main(String[] args) {
        try {
            AgentCli cli = new AgentCli(args);
            Agent agent = cli.agent;
            agent.start();
        } catch (Exception e) {
            exitWithError(LOGGER, "Could not start Agent!", e);
        }
    }
}
