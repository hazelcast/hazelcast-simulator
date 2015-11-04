/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.jclouds.compute.ComputeService;

import java.io.File;

import static com.hazelcast.simulator.utils.CliUtils.initOptionsWithHelp;
import static com.hazelcast.simulator.utils.CliUtils.printHelpAndExit;

final class CloudInfoCli {

    private final OptionParser parser = new OptionParser();

    private final OptionSpec showLocationsSpec = parser.accepts("showLocations",
            "Shows all locations available. In Amazon for example this would be regions and zones.");

    private final OptionSpec showHardwareSpec = parser.accepts("showHardware",
            "Shows all hardware available");

    private final OptionSpec showImagesSpec = parser.accepts("showImages",
            "Shows all images available");

    private final OptionSpec verboseSpec = parser.accepts("verbose",
            "Shows very detailed info");

    private final OptionSpec<String> locationSpec = parser.accepts("location",
            "The locationId.")
            .withRequiredArg().ofType(String.class);

    private final OptionSpec<String> propertiesFileSpec = parser.accepts("propertiesFile",
            "The file containing the simulator properties. If no file is explicitly configured, first the working directory is "
                    + "checked for a file 'simulator.properties'. All missing properties are always loaded from "
                    + "'$SIMULATOR_HOME/conf/simulator.properties'.")
            .withRequiredArg().ofType(String.class);

    private CloudInfoCli() {
    }

    static CloudInfo init(String[] args) {
        CloudInfoCli cli = new CloudInfoCli();
        OptionSet options = initOptionsWithHelp(cli.parser, args);

        SimulatorProperties simulatorProperties = new SimulatorProperties();
        simulatorProperties.init(getPropertiesFile(cli, options));

        ComputeService computeService = new ComputeServiceBuilder(simulatorProperties).build();

        return new CloudInfo(options.valueOf(cli.locationSpec), options.has(cli.verboseSpec), computeService);
    }

    static void run(String[] args, CloudInfo cloudInfo) {
        CloudInfoCli cli = new CloudInfoCli();
        OptionSet options = initOptionsWithHelp(cli.parser, args);

        try {
            if (options.has(cli.showLocationsSpec)) {
                cloudInfo.showLocations();
            } else if (options.has(cli.showHardwareSpec)) {
                cloudInfo.showHardware();
            } else if (options.has(cli.showImagesSpec)) {
                cloudInfo.showImages();
            } else {
                printHelpAndExit(cli.parser);
            }
        } finally {
            cloudInfo.shutdown();
        }
    }

    private static File getPropertiesFile(CloudInfoCli cli, OptionSet options) {
        if (options.has(cli.propertiesFileSpec)) {
            // a file was explicitly configured
            return new File(options.valueOf(cli.propertiesFileSpec));
        } else {
            return null;
        }
    }
}
