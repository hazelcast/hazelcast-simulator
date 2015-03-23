package com.hazelcast.simulator.provisioner;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.log4j.Logger;

import java.io.File;

import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.FileUtils.newFile;

public class ProvisionerCli {

    private static final Logger LOGGER = Logger.getLogger(ProvisionerCli.class);

    private final OptionParser parser = new OptionParser();

    private final OptionSpec<String> gitSpec = parser.accepts("git",
            "Overrides the HAZELCAST_VERSION_SPEC property and forces Provisioner to build Hazelcast JARs from a given GIT "
                    + "version. This makes it easier to run a test with different versions of Hazelcast, e.g.\n"
                    + "     --git f0288f713                to use the Git revision f0288f713\n"
                    + "     --git myRepository/myBranch    to use branch myBranch from a repository myRepository.\n"
                    + "You can specify custom repositories in 'simulator.properties'."
    ).withRequiredArg().ofType(String.class);

    private final OptionSpec restartSpec = parser.accepts("restart",
            "Restarts all agents");

    private final OptionSpec<String> downloadSpec = parser.accepts("download",
            "Download all the files from the workers directory. To delete all worker directories, run with --clean"
    ).withOptionalArg().defaultsTo("workers").ofType(String.class);

    private final OptionSpec cleanSpec = parser.accepts("clean",
            "Cleans the workers directories.");

    private final OptionSpec<Integer> scaleSpec = parser.accepts("scale",
            "Number of agent machines to scale to. If the number of machines already exists, the call is ignored. If the "
                    + "desired number of machines is smaller than the actual number of machines, machines are terminated."
    ).withRequiredArg().ofType(Integer.class);

    private final OptionSpec terminateSpec = parser.accepts("terminate",
            "Terminate all agent machines in the provisioner");

    private final OptionSpec killSpec = parser.accepts("kill",
            "Kill all the agent processes (it will do a killall -9 java and kills all java processes)");

    private final OptionSpec listAgentsSpec = parser.accepts("listAgents",
            "Lists the running agents.");

    private final OptionSpec<String> propertiesFileSpec = parser.accepts("propertiesFile",
            "The file containing the simulator properties. If no file is explicitly configured, first the working directory is "
                    + "checked for a file 'simulator.properties'. All missing properties are always loaded from "
                    + "'$SIMULATOR_HOME/conf/simulator.properties'."
    ).withRequiredArg().ofType(String.class);

    private final OptionSpec<Boolean> enterpriseEnabledSpec = parser.accepts("enterpriseEnabled",
            "Use hazelcast enterprise edition JARs."
    ).withRequiredArg().ofType(Boolean.class).defaultsTo(false);

    private final OptionSpec helpSpec = parser.accepts("help", "Show help").forHelp();

    private final Provisioner provisioner;
    private OptionSet options;

    public ProvisionerCli(Provisioner provisioner) {
        this.provisioner = provisioner;
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

        provisioner.props.init(getPropertiesFile());

        if (options.has(gitSpec)) {
            String git = options.valueOf(gitSpec);
            provisioner.props.forceGit(git);
        }

        provisioner.init();
        if (options.has(restartSpec)) {
            boolean enterpriseEnabled = options.valueOf(enterpriseEnabledSpec);
            provisioner.restart(enterpriseEnabled);
            provisioner.startAgents();
        } else if (options.has(killSpec)) {
            provisioner.killAgents();
        } else if (options.has(downloadSpec)) {
            String dir = options.valueOf(downloadSpec);
            provisioner.download(dir);
        } else if (options.has(cleanSpec)) {
            provisioner.clean();
        } else if (options.has(terminateSpec)) {
            provisioner.terminate();
        } else if (options.has(scaleSpec)) {
            int size = options.valueOf(scaleSpec);
            boolean enterpriseEnabled = options.valueOf(enterpriseEnabledSpec);
            provisioner.scale(size, enterpriseEnabled);
        } else if (options.has(listAgentsSpec)) {
            provisioner.listAgents();
        } else {
            parser.printHelpOn(System.out);
        }
    }

    private File getPropertiesFile() {
        if (options.has(propertiesFileSpec)) {
            // a file was explicitly configured
            return newFile(options.valueOf(propertiesFileSpec));
        } else {
            return null;
        }
    }
}
