package com.hazelcast.simulator.provisioner;

import com.hazelcast.simulator.common.AgentsFile;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.File;

import static com.hazelcast.simulator.common.SimulatorProperties.PROPERTIES_FILE_NAME;
import static com.hazelcast.simulator.utils.CliUtils.initOptionsWithHelp;
import static com.hazelcast.simulator.utils.CliUtils.printHelpAndExit;
import static com.hazelcast.simulator.utils.FileUtils.newFile;

final class ProvisionerCli {

    private final OptionParser parser = new OptionParser();

    private final OptionSpec<String> gitSpec = parser.accepts("git",
            "Overrides the HAZELCAST_VERSION_SPEC property and forces Provisioner to build Hazelcast JARs from a given GIT"
                    + " version. This makes it easier to run a test with different versions of Hazelcast, e.g.\n"
                    + "     --git f0288f713                to use the Git revision f0288f713\n"
                    + "     --git myRepository/myBranch    to use branch myBranch from a repository myRepository.\n"
                    + "You can specify custom repositories in 'simulator.properties'.")
            .withRequiredArg().ofType(String.class);

    private final OptionSpec<Integer> scaleSpec = parser.accepts("scale",
            "Number of Simulator machines to scale to. If the number of machines already exists, the call is ignored. If the"
                    + " desired number of machines is smaller than the actual number of machines, machines are terminated.")
            .withRequiredArg().ofType(Integer.class);

    private final OptionSpec installSpec = parser.accepts("install",
            "Installs Simulator on all provisioned machines.");

    private final OptionSpec listAgentsSpec = parser.accepts("list",
            "Lists the provisioned machines (from " + AgentsFile.NAME + " file).");

    private final OptionSpec<String> downloadSpec = parser.accepts("download",
            "Download all files from the remote Worker directories. Use --clean to delete all Worker directories.")
            .withOptionalArg().defaultsTo("workers").ofType(String.class);

    private final OptionSpec cleanSpec = parser.accepts("clean",
            "Cleans the remote Worker directories on the provisioned machines.");

    private final OptionSpec killSpec = parser.accepts("kill",
            "Kills the Java processes on all provisioned machines (via killall -9 java).");

    private final OptionSpec terminateSpec = parser.accepts("terminate",
            "Terminates all provisioned machines.");

    private final OptionSpec<String> propertiesFileSpec = parser.accepts("propertiesFile",
            "The file containing the Simulator properties. If no file is explicitly configured, first the local working directory"
                    + " is checked for a file '" + PROPERTIES_FILE_NAME + "'. All missing properties are always loaded from"
                    + " '$SIMULATOR_HOME/conf/" + PROPERTIES_FILE_NAME + "'.")
            .withRequiredArg().ofType(String.class);

    private final OptionSpec<Boolean> enterpriseEnabledSpec = parser.accepts("enterpriseEnabled",
            "Use JARs of Hazelcast Enterprise Edition.")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(false);

    private final Provisioner provisioner;
    private final OptionSet options;

    ProvisionerCli(Provisioner provisioner, String[] args) {
        this.provisioner = provisioner;
        this.options = initOptionsWithHelp(parser, args);
    }

    void init() {
        provisioner.props.init(getPropertiesFile());

        if (options.has(gitSpec)) {
            String git = options.valueOf(gitSpec);
            provisioner.props.forceGit(git);
        }
    }

    void run() {
        try {
            provisioner.init();
            if (options.has(scaleSpec)) {
                int size = options.valueOf(scaleSpec);
                boolean enterpriseEnabled = options.valueOf(enterpriseEnabledSpec);
                provisioner.scale(size, enterpriseEnabled);
            } else if (options.has(installSpec)) {
                boolean enterpriseEnabled = options.valueOf(enterpriseEnabledSpec);
                provisioner.installSimulator(enterpriseEnabled);
            } else if (options.has(listAgentsSpec)) {
                provisioner.listMachines();
            } else if (options.has(downloadSpec)) {
                String dir = options.valueOf(downloadSpec);
                provisioner.download(dir);
            } else if (options.has(cleanSpec)) {
                provisioner.clean();
            } else if (options.has(killSpec)) {
                provisioner.killJavaProcessed();
            } else if (options.has(terminateSpec)) {
                provisioner.terminate();
            } else {
                printHelpAndExit(parser);
            }
        } finally {
            provisioner.shutdown();
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
