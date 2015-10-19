package com.hazelcast.simulator.provisioner;

import com.hazelcast.simulator.common.AgentsFile;
import com.hazelcast.simulator.common.SimulatorProperties;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import static com.hazelcast.simulator.common.SimulatorProperties.PROPERTIES_FILE_NAME;
import static com.hazelcast.simulator.utils.CliUtils.initOptionsWithHelp;
import static com.hazelcast.simulator.utils.CliUtils.printHelpAndExit;
import static com.hazelcast.simulator.utils.SimulatorUtils.loadSimulatorProperties;

final class ProvisionerCli {

    private final OptionParser parser = new OptionParser();

    private final OptionSpec<String> gitSpec = parser.accepts("git",
            "Overrides the HAZELCAST_VERSION_SPEC property and forces Provisioner to build Hazelcast JARs from a given Git"
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

    private ProvisionerCli() {
    }

    static Provisioner init(String[] args) {
        ProvisionerCli cli = new ProvisionerCli();
        OptionSet options = initOptionsWithHelp(cli.parser, args);

        SimulatorProperties properties = loadSimulatorProperties(options, cli.propertiesFileSpec);
        if (options.has(cli.gitSpec)) {
            String git = options.valueOf(cli.gitSpec);
            properties.forceGit(git);
        }

        return new Provisioner(properties);
    }

    static void run(String[] args, Provisioner provisioner) {
        ProvisionerCli cli = new ProvisionerCli();
        OptionSet options = initOptionsWithHelp(cli.parser, args);

        try {
            if (options.has(cli.scaleSpec)) {
                int size = options.valueOf(cli.scaleSpec);
                boolean enterpriseEnabled = options.valueOf(cli.enterpriseEnabledSpec);
                provisioner.scale(size, enterpriseEnabled);
            } else if (options.has(cli.installSpec)) {
                boolean enterpriseEnabled = options.valueOf(cli.enterpriseEnabledSpec);
                provisioner.installSimulator(enterpriseEnabled);
            } else if (options.has(cli.listAgentsSpec)) {
                provisioner.listMachines();
            } else if (options.has(cli.downloadSpec)) {
                String dir = options.valueOf(cli.downloadSpec);
                provisioner.download(dir);
            } else if (options.has(cli.cleanSpec)) {
                provisioner.clean();
            } else if (options.has(cli.killSpec)) {
                provisioner.killJavaProcesses();
            } else if (options.has(cli.terminateSpec)) {
                provisioner.terminate();
            } else {
                printHelpAndExit(cli.parser);
            }
        } finally {
            provisioner.shutdown();
        }
    }
}
