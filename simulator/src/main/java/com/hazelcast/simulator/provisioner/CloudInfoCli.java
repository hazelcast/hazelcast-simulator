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

import com.hazelcast.simulator.utils.CliUtils;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.File;

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

    private final CloudInfo cloudInfo;
    private final OptionSet options;

    CloudInfoCli(CloudInfo cloudInfo, String[] args) {
        this.cloudInfo = cloudInfo;
        this.options = CliUtils.initOptionsWithHelp(parser, args);
    }

    void run() {
        try {
            cloudInfo.props.init(getPropertiesFile());

            cloudInfo.locationId = options.valueOf(locationSpec);
            cloudInfo.verbose = options.has(verboseSpec);

            if (options.has(showLocationsSpec)) {
                cloudInfo.init();
                cloudInfo.showLocations();
            } else if (options.has(showHardwareSpec)) {
                cloudInfo.init();
                cloudInfo.showHardware();
            } else if (options.has(showImagesSpec)) {
                cloudInfo.init();
                cloudInfo.showImages();
            } else {
                CliUtils.printHelpAndExit(parser);
            }
        } finally {
            cloudInfo.shutdown();
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
