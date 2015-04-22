package com.hazelcast.simulator.provisioner;

import com.hazelcast.simulator.common.AgentsFile;
import com.hazelcast.simulator.utils.CliUtils;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.File;

import static com.hazelcast.simulator.utils.FileUtils.newFile;

class AwsProvisionerCli {

    private final OptionParser parser = new OptionParser();

    private final OptionSpec<String> makeLB = parser.accepts("newLb",
            "Create new load balancer if it dose not exist.")
            .withRequiredArg().ofType(String.class);

    private final OptionSpec<String> agentsToLB = parser.accepts("addToLb",
            "Adds the ips in '" + AgentsFile.NAME + "' file to the load balancer.")
            .withRequiredArg().ofType(String.class);

    private final OptionSpec<Integer> scale = parser.accepts("scale",
            "Desired number of machines to scale to.")
            .withRequiredArg().ofType(Integer.class);

    private final OptionSpec<String> propertiesFile = parser.accepts("propertiesFile",
            "The file containing the simulator properties. If no file is explicitly configured, first the working directory is "
                    + "checked for a file 'simulator.properties'.")
            .withRequiredArg().ofType(String.class);

    private final AwsProvisioner aws;
    private final OptionSet options;

    AwsProvisionerCli(AwsProvisioner awsProvisioner, String[] args) {
        this.aws = awsProvisioner;
        this.options = CliUtils.initOptionsWithHelp(parser, args);
    }

    void run() {
        try {
            if (options.has(propertiesFile)) {
                File file = newFile(options.valueOf(propertiesFile));
                aws.setProperties(file);
            }

            if (options.has(scale)) {
                int count = options.valueOf(scale);
                aws.scaleInstanceCountTo(count);
            } else if (options.has(makeLB)) {
                String name = options.valueOf(makeLB);
                aws.createLoadBalancer(name);
            } else if (options.has(agentsToLB)) {
                String name = options.valueOf(agentsToLB);
                aws.addAgentsToLoadBalancer(name);
            }
        } finally {
            aws.shutdown();
        }
    }
}
