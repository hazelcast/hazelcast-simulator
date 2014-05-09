package com.hazelcast.stabilizer.provisioner;

import com.hazelcast.logging.ILogger;
import com.hazelcast.stabilizer.Utils;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.File;

import static com.hazelcast.stabilizer.Utils.getStablizerHome;
import static java.lang.String.format;

public class CloudInfoCli {

    private final static File STABILIZER_HOME = getStablizerHome();
    private final static ILogger log = com.hazelcast.logging.Logger.getLogger(ProvisionerCli.class);

    public final OptionParser parser = new OptionParser();

    public final OptionSpec showLocationsSpec = parser.accepts("showLocations",
            "Shows all locations available. In Amazon for example this would be ...");

    public final OptionSpec showHardwareSpec = parser.accepts("showHardware",
            "Shows all hardware available");

    public final OptionSpec showImagesSpec = parser.accepts("showImages",
            "Shows all images available");

    public final OptionSpec verboseSpec = parser.accepts("verbose",
            "Shows very detailed info");

    public final OptionSpec<String> locationSpec = parser.accepts("location",
            "The locationId.")
            .withRequiredArg().ofType(String.class);

    public final OptionSpec helpSpec = parser.accepts("help", "Show help").forHelp();

    private final OptionSpec<String> propertiesFileSpec = parser.accepts("propertiesFile",
            "The file containing the stabilizer properties. If no file is explicitly configured, first the " +
                    "working directory is checked for a file 'stabilizer.properties'. If that doesn't exist, then" +
                    "the STABILIZER_HOME/conf/stabilizer.properties is loaded."
    )
            .withRequiredArg().ofType(String.class);

    private final CloudInfo cloudInfo;

    public CloudInfoCli(CloudInfo cloudInfo) {
        this.cloudInfo = cloudInfo;
    }

    public void run(String[] args) throws Exception {
        OptionSet options;
        try {
            options = parser.parse(args);
        } catch (OptionException e) {
            Utils.exitWithError(log, e.getMessage() + ". Use --help to get overview of the help options.");
            return;//
        }

        if (options.has(helpSpec)) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        File propertiesFile = getPropertiesFile(options);
        cloudInfo.props.load(propertiesFile);
        log.info(format("stabilizer.properties: %s", cloudInfo.props.getFile().getAbsolutePath()));


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
        }else{
            parser.printHelpOn(System.out);
        }
    }

    private File getPropertiesFile(OptionSet options) {
        File file;
        if (options.has(propertiesFileSpec)) {
            //a file was explicitly configured
            file = new File(options.valueOf(propertiesFileSpec));
        } else {
            //look in the working directory first
            file = new File("stabilizer.properties");
            if (!file.exists()) {
                //if not exist, then look in the conf directory.
                file = Utils.toFile(STABILIZER_HOME, "conf", "stabilizer.properties");
            }
        }

        if (!file.exists()) {
            Utils.exitWithError(log, "Could not find stabilizer.properties file:  " + file.getAbsolutePath());
        }

        return file;
    }
}
