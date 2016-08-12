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
package com.hazelcast.simulator.provisioner;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.utils.Bash;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.log4j.Logger;
import org.jclouds.compute.ComputeService;

import static com.hazelcast.simulator.common.GitInfo.getBuildTime;
import static com.hazelcast.simulator.common.GitInfo.getCommitIdAbbrev;
import static com.hazelcast.simulator.common.SimulatorProperties.PROPERTIES_FILE_NAME;
import static com.hazelcast.simulator.utils.CliUtils.initOptionsWithHelp;
import static com.hazelcast.simulator.utils.CliUtils.printHelpAndExit;
import static com.hazelcast.simulator.utils.CloudProviderUtils.isCloudProvider;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.SimulatorUtils.loadSimulatorProperties;
import static java.lang.String.format;

final class ProvisionerCli {
    private static final Logger LOGGER = Logger.getLogger(ProvisionerCli.class);

    // open for testing
    Provisioner provisioner;

    private final OptionParser parser = new OptionParser();

    private final OptionSpec<Integer> scaleSpec = parser.accepts("scale",
            "Number of Simulator machines to scale to. If the number of machines already exists, the call is ignored. If the"
                    + " desired number of machines is smaller than the actual number of machines, machines are terminated.")
            .withRequiredArg().ofType(Integer.class);

    private final OptionSpec installJavaSpec = parser.accepts("installJava",
            "Installs JAVA on all provisioned machines.");

    private final OptionSpec installSpec = parser.accepts("install",
            "Installs Simulator on all provisioned machines.");

    private final OptionSpec<String> downloadSpec = parser.accepts("download",
            "Download all files from the remote Worker directories. Use --clean to delete all Worker directories.")
            .withOptionalArg().ofType(String.class).defaultsTo("workers");

    private final OptionSpec cleanSpec = parser.accepts("clean",
            "Cleans the remote Worker directories on the provisioned machines.");

    private final OptionSpec killSpec = parser.accepts("kill",
            "Kills the Java processes on all provisioned machines (via killall -9 java).");

    private final OptionSpec terminateSpec = parser.accepts("terminate",
            "Terminates all provisioned machines.");

    private final OptionSpec<String> propertiesFileSpec = parser.accepts("propertiesFile",
            "The file containing the Simulator properties. If no file is explicitly configured, first the local working directory"
                    + " is checked for a file '" + PROPERTIES_FILE_NAME + "'. All missing properties are always loaded from"
                    + " '$SIMULATOR_HOME/conf/" + PROPERTIES_FILE_NAME + "'.")
            .withRequiredArg().ofType(String.class);

    private final OptionSet options;

    ProvisionerCli(String[] args) {
        this.options = initOptionsWithHelp(parser, args);

        SimulatorProperties properties = loadSimulatorProperties(options, propertiesFileSpec);
        ComputeService computeService = isCloudProvider(properties) ? new ComputeServiceBuilder(properties).build() : null;
        Bash bash = new Bash(properties);

        this.provisioner = new Provisioner(properties, computeService, bash);
    }

    public void run() {
        try {
            if (options.has(scaleSpec)) {
                int size = options.valueOf(scaleSpec);
                provisioner.scale(size);
            } else if (options.has(installJavaSpec)) {
                provisioner.installJava();
            } else if (options.has(installSpec)) {
                provisioner.installSimulator();
            } else if (options.has(downloadSpec)) {
                String dir = options.valueOf(downloadSpec);
                provisioner.download(dir);
            } else if (options.has(cleanSpec)) {
                provisioner.clean();
            } else if (options.has(killSpec)) {
                provisioner.killJavaProcesses();
            } else if (options.has(terminateSpec)) {
                provisioner.terminate();
            } else {
                printHelpAndExit(parser);
            }
        } finally {
            provisioner.shutdown();
        }
    }

    public static void main(String[] args) {
        LOGGER.info("Hazelcast Simulator Provisioner");
        LOGGER.info(format("Version: %s, Commit: %s, Build Time: %s",
                getSimulatorVersion(), getCommitIdAbbrev(), getBuildTime()));
        LOGGER.info(format("SIMULATOR_HOME: %s", getSimulatorHome()));

        try {
            ProvisionerCli cli = new ProvisionerCli(args);
            cli.run();
        } catch (Exception e) {
            exitWithError(LOGGER, "Could not execute command", e);
        }
    }
}
