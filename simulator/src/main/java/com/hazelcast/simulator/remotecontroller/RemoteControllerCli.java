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
package com.hazelcast.simulator.remotecontroller;

import com.hazelcast.simulator.common.SimulatorProperties;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;

import static com.hazelcast.simulator.common.GitInfo.getBuildTime;
import static com.hazelcast.simulator.common.GitInfo.getCommitIdAbbrev;
import static com.hazelcast.simulator.utils.CliUtils.initOptionsWithHelp;
import static com.hazelcast.simulator.utils.CliUtils.printHelpAndExit;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static java.lang.String.format;

final class RemoteControllerCli {
    private static final Logger LOGGER = Logger.getLogger(RemoteControllerCli.class);

    // open for testing purposes
    RemoteController remoteController;

    private final OptionParser parser = new OptionParser();

    private final OptionSpec<String> propertiesFileSpec = parser.accepts("propertiesFile",
            "The file containing the simulator properties. If no file is explicitly configured, first the working directory is "
                    + "checked for a file 'simulator.properties'. All missing properties are always loaded from "
                    + "'$SIMULATOR_HOME/conf/simulator.properties'.")
            .withRequiredArg().ofType(String.class);

    private final OptionSpec quietSpec = parser.accepts("quiet",
            "Just prints the command response to the console.");

    private final OptionSpec listComponentsSpec = parser.accepts("listComponents",
            "Lists all running Simulator Components");

    private final OptionSet options;

    RemoteControllerCli(String[] args) {
        options = initOptionsWithHelp(parser, args);

        boolean isQuiet = options.has(quietSpec);
        if (isQuiet) {
            Logger.getRootLogger().setLevel(Level.WARN);
        }

        SimulatorProperties simulatorProperties = new SimulatorProperties();
        simulatorProperties.init(getPropertiesFile());

        this.remoteController = new RemoteController(simulatorProperties, isQuiet);
    }

    void run() {
        try {
            remoteController.start();
            if (options.has(listComponentsSpec)) {
                remoteController.listComponents();
            } else {
                printHelpAndExit(parser);
            }
        } finally {
            remoteController.shutdown();
        }
    }

    public static void main(String[] args) {
        LOGGER.info("Hazelcast Simulator Remote Controller");
        LOGGER.info(
                format("Version: %s, Commit: %s, Build Time: %s", getSimulatorVersion(), getCommitIdAbbrev(), getBuildTime()));
        LOGGER.info(format("SIMULATOR_HOME: %s", getSimulatorHome().getAbsolutePath()));

        try {
            RemoteControllerCli cli = new RemoteControllerCli(args);
            cli.run();
        } catch (Exception e) {
            exitWithError(LOGGER, "Error during execution of Remote Controller!", e);
        }
    }

    private File getPropertiesFile() {
        if (options.has(propertiesFileSpec)) {
            // a file was explicitly configured
            return new File(options.valueOf(propertiesFileSpec));
        } else {
            return null;
        }
    }
}
