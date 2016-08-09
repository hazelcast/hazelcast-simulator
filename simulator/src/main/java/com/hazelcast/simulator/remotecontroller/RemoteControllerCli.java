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

import static com.hazelcast.simulator.utils.CliUtils.initOptionsWithHelp;
import static com.hazelcast.simulator.utils.CliUtils.printHelpAndExit;

final class RemoteControllerCli {

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

    private RemoteControllerCli() {
    }

    static RemoteController init(String[] args) {
        RemoteControllerCli cli = new RemoteControllerCli();
        OptionSet options = initOptionsWithHelp(cli.parser, args);

        boolean isQuiet = options.has(cli.quietSpec);
        if (isQuiet) {
            Logger.getRootLogger().setLevel(Level.WARN);
        }

        RemoteController.logHeader();

        SimulatorProperties simulatorProperties = new SimulatorProperties();
        simulatorProperties.init(getPropertiesFile(cli, options));

        return new RemoteController(simulatorProperties, isQuiet);
    }

    static void run(String[] args, RemoteController remoteController) {
        RemoteControllerCli cli = new RemoteControllerCli();
        OptionSet options = initOptionsWithHelp(cli.parser, args);

        try {
            remoteController.start();
            if (options.has(cli.listComponentsSpec)) {
                remoteController.listComponents();
            } else {
                printHelpAndExit(cli.parser);
            }
        } finally {
            remoteController.shutdown();
        }
    }

    private static File getPropertiesFile(RemoteControllerCli cli, OptionSet options) {
        if (options.has(cli.propertiesFileSpec)) {
            // a file was explicitly configured
            return new File(options.valueOf(cli.propertiesFileSpec));
        } else {
            return null;
        }
    }
}
