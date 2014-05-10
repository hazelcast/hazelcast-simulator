package com.hazelcast.stabilizer.provisioner;

import com.hazelcast.logging.ILogger;
import com.hazelcast.stabilizer.Utils;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.File;

import static java.lang.String.format;

public class ProvisionerCli {
    private final static ILogger log = com.hazelcast.logging.Logger.getLogger(ProvisionerCli.class);

    public final OptionParser parser = new OptionParser();

    public final OptionSpec restartSpec = parser.accepts("restart",
            "Restarts all agents");

    public final OptionSpec downloadSpec = parser.accepts("download",
            "Download all the files from the workers directory. " +
                    "To delete all worker directories, run with --clean"
    );

    public final OptionSpec cleanSpec = parser.accepts("clean",
            "Cleans the workers directories. ");

    public final OptionSpec<Integer> scaleSpec = parser.accepts("scale",
            "Number of agent machines to scale to. If the number of machines already exists, the call is ignored. If the " +
                    "desired number of machines is smaller than the actual number of machines, machines are terminated."
    )
            .withRequiredArg().ofType(Integer.class);

    public final OptionSpec terminateSpec = parser.accepts("terminateWorker",
            "Terminate all agent machines in the provisioner");

    public final OptionSpec killSpec = parser.accepts("kill",
            "Kill all the agent processes");

    public final OptionSpec helpSpec = parser.accepts("help", "Show help").forHelp();

    private final OptionSpec<String> propertiesFileSpec = parser.accepts("propertiesFile",
            "The file containing the stabilizer properties. If no file is explicitly configured, first the " +
                    "working directory is checked for a file 'stabilizer.properties'. If that doesn't exist, then" +
                    "the STABILIZER_HOME/conf/stabilizer.properties is loaded."
    )
            .withRequiredArg().ofType(String.class);

    private final Provisioner provisioner;
    private OptionSet options;

    public ProvisionerCli(Provisioner provisioner) {
        this.provisioner = provisioner;
    }

    public void run(String[] args) throws Exception {
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

        provisioner.props.init(getPropertiesFile());
        log.info(format("stabilizer.properties: %s", provisioner.props.getFile().getAbsolutePath()));

        provisioner.init();

        if (options.has(restartSpec)) {
            provisioner.restart();
            provisioner.startAgents();
        } else if (options.has(killSpec)) {
            provisioner.killAgents();
        } else if (options.has(downloadSpec)) {
            provisioner.download();
        } else if (options.has(cleanSpec)) {
            provisioner.clean();
        } else if (options.has(terminateSpec)) {
            provisioner.terminate();
        } else if (options.has(scaleSpec)) {
            int size = options.valueOf(scaleSpec);
            provisioner.scale(size);
        }
    }

    private File getPropertiesFile() {
        if (options.has(propertiesFileSpec)) {
            //a file was explicitly configured
            return new File(options.valueOf(propertiesFileSpec));
        } else {
            return null;
        }
    }
}
