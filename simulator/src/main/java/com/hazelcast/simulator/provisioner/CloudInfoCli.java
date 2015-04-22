package com.hazelcast.simulator.provisioner;

import com.hazelcast.simulator.utils.CliUtils;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.File;

public class CloudInfoCli {

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

    void run() throws Exception {
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
            parser.printHelpOn(System.out);
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
