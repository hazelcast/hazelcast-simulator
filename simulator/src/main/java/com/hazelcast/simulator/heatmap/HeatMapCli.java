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
package com.hazelcast.simulator.heatmap;

import com.hazelcast.simulator.common.SimulatorProperties;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.File;

import static com.hazelcast.simulator.utils.CliUtils.initOptionsWithHelp;

final class HeatMapCli {

    private final OptionParser parser = new OptionParser();

    private final OptionSpec<String> directorySpec = parser.accepts("directory",
            "Defines the working directory in which the histogram files are searched.")
            .withRequiredArg().ofType(String.class).defaultsTo("");

    private final OptionSpec<String> testNameSpec = parser.accepts("testName",
            "Name of the test to search for.")
            .withRequiredArg().ofType(String.class).defaultsTo("");

    private final OptionSpec<String> probeNameSpec = parser.accepts("probeName",
            "Name of the probe to search for.")
            .withRequiredArg().ofType(String.class).defaultsTo("");

    private final OptionSpec<String> propertiesFileSpec = parser.accepts("propertiesFile",
            "The file containing the simulator properties. If no file is explicitly configured, first the working directory is "
                    + "checked for a file 'simulator.properties'. All missing properties are always loaded from "
                    + "'$SIMULATOR_HOME/conf/simulator.properties'.")
            .withRequiredArg().ofType(String.class);

    private HeatMapCli() {
    }

    static HeatMap init(String[] args) {
        HeatMap.logHeader();

        HeatMapCli cli = new HeatMapCli();
        OptionSet options = initOptionsWithHelp(cli.parser, args);

        SimulatorProperties simulatorProperties = new SimulatorProperties();
        simulatorProperties.init(getPropertiesFile(cli, options));

        return new HeatMap(options.valueOf(cli.directorySpec), options.valueOf(cli.testNameSpec),
                options.valueOf(cli.probeNameSpec));
    }

    static void run(HeatMap heatMap) {
        heatMap.createHeatMap();
    }

    private static File getPropertiesFile(HeatMapCli cli, OptionSet options) {
        if (options.has(cli.propertiesFileSpec)) {
            // a file was explicitly configured
            return new File(options.valueOf(cli.propertiesFileSpec));
        } else {
            return null;
        }
    }
}
