package com.hazelcast.simulator.provisioner;

import com.hazelcast.simulator.common.AgentsFile;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import static com.hazelcast.simulator.common.SimulatorProperties.PROPERTIES_FILE_NAME;
import static com.hazelcast.simulator.utils.CliUtils.initOptionsWithHelp;
import static com.hazelcast.simulator.utils.SimulatorUtils.loadSimulatorProperties;
import static java.lang.String.format;

final class AwsProvisionerCli {

    private final OptionParser parser = new OptionParser();

    private final OptionSpec<String> createLoadBalancerSpec = parser.accepts("newLb",
            "Create new load balancer if it dose not exist.")
            .withRequiredArg().ofType(String.class);

    private final OptionSpec<String> addAgentsToLoadBalancer = parser.accepts("addToLb",
            "Adds the IP addresses in '" + AgentsFile.NAME + "' file to the load balancer.")
            .withRequiredArg().ofType(String.class);

    private final OptionSpec<Integer> scaleSpec = parser.accepts("scale",
            "Desired number of machines to scale to.")
            .withRequiredArg().ofType(Integer.class);

    private final OptionSpec<String> propertiesFileSpec = parser.accepts("propertiesFile",
            format("The file containing the simulator properties. If no file is explicitly configured,"
                            + " first the working directory is checked for a file '%s'."
                            + " All missing properties are always loaded from SIMULATOR_HOME/conf/%s",
                    PROPERTIES_FILE_NAME, PROPERTIES_FILE_NAME))
            .withRequiredArg().ofType(String.class);

    private AwsProvisionerCli() {
    }

    static AwsProvisioner init(String[] args) {
        AwsProvisionerCli cli = new AwsProvisionerCli();
        OptionSet options = initOptionsWithHelp(cli.parser, args);

        return new AwsProvisioner(loadSimulatorProperties(options, cli.propertiesFileSpec));
    }

    static void run(String[] args, AwsProvisioner provisioner) {
        AwsProvisionerCli cli = new AwsProvisionerCli();
        OptionSet options = initOptionsWithHelp(cli.parser, args);

        try {
            if (options.has(cli.scaleSpec)) {
                int count = options.valueOf(cli.scaleSpec);
                provisioner.scaleInstanceCountTo(count);
            } else if (options.has(cli.createLoadBalancerSpec)) {
                String name = options.valueOf(cli.createLoadBalancerSpec);
                provisioner.createLoadBalancer(name);
            } else if (options.has(cli.addAgentsToLoadBalancer)) {
                String name = options.valueOf(cli.addAgentsToLoadBalancer);
                provisioner.addAgentsToLoadBalancer(name);
            }
        } finally {
            provisioner.shutdown();
        }
    }
}
