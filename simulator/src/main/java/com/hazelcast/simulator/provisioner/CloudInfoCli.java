package com.hazelcast.simulator.provisioner;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.log4j.Logger;

import java.io.File;

import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;

public class CloudInfoCli {

    private static final Logger LOGGER = Logger.getLogger(ProvisionerCli.class);

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
            "The locationId."
    ).withRequiredArg().ofType(String.class);

    private final OptionSpec<String> propertiesFileSpec = parser.accepts("propertiesFile",
            "The file containing the simulator properties. If no file is explicitly configured, first the working directory is "
                    + "checked for a file 'simulator.properties'. All missing properties are always loaded from "
                    + "'$SIMULATOR_HOME/conf/simulator.properties'."
    ).withRequiredArg().ofType(String.class);

    private final OptionSpec helpSpec = parser.accepts("help", "Show help").forHelp();

    private final CloudInfo cloudInfo;
    private OptionSet options;

    public CloudInfoCli(CloudInfo cloudInfo) {
        this.cloudInfo = cloudInfo;
    }

    public void run(String[] args) throws Exception {
        try {
            options = parser.parse(args);
        } catch (OptionException e) {
            exitWithError(LOGGER, e.getMessage() + ". Use --help to get overview of the help options.");
            return;
        }

        if (options.has(helpSpec)) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

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
