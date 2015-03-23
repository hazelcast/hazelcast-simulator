package com.hazelcast.simulator.provisioner;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.log4j.Logger;

import java.io.File;

import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.FileUtils.newFile;

public class AwsProvisionerCli {

    private static final Logger LOGGER = Logger.getLogger(AwsProvisionerCli.class);

    private final OptionParser parser = new OptionParser();

    private final OptionSpec<String> makeLB = parser.accepts("newLb",
            "Create new load balancer if it dose not exist."
    ).withRequiredArg().ofType(String.class);

    private final OptionSpec<String> agentsToLB = parser.accepts("addToLb",
            "Adds the ips in '" + Provisioner.AGENTS_FILE + "' file to the load balancer."
    ).withRequiredArg().ofType(String.class);

    private final OptionSpec<Integer> scale = parser.accepts("scale",
            "Desired number of machines to scale to."
    ).withRequiredArg().ofType(Integer.class);

    private final OptionSpec<String> propertiesFile = parser.accepts("propertiesFile",
            "The file containing the simulator properties. If no file is explicitly configured, first the working directory is "
                    + "checked for a file 'simulator.properties'."
    ).withRequiredArg().ofType(String.class);

    private final OptionSpec help = parser.accepts("help", "Show help").forHelp();

    private final AwsProvisioner aws;
    private OptionSet options;

    public AwsProvisionerCli(AwsProvisioner awsProvisioner) {
        this.aws = awsProvisioner;
    }

    public void run(String[] args) throws Exception {
        try {
            options = parser.parse(args);
        } catch (OptionException e) {
            exitWithError(LOGGER, e.getMessage() + ". Use --help to get overview of the help options.");
        }

        if (options.has(help)) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        if (options.has(propertiesFile)) {
            File file = newFile(options.valueOf(propertiesFile));
            aws.setProperties(file);
        }

        if (options.has(scale)) {
            int count = options.valueOf(scale);
            aws.scaleInstanceCountTo(count);
            System.exit(0);
        }

        if (options.has(makeLB)) {
            String name = options.valueOf(makeLB);
            aws.createLoadBalancer(name);
            System.exit(0);
        }

        if (options.has(agentsToLB)) {
            String name = options.valueOf(agentsToLB);
            aws.addAgentsToLoadBalancer(name);
            System.exit(0);
        }
    }
}
