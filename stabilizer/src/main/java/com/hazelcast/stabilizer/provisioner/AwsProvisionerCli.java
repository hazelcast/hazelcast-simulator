package com.hazelcast.stabilizer.provisioner;

import com.hazelcast.stabilizer.probes.probes.util.Utils;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.log4j.Logger;

import java.io.File;

import static com.hazelcast.stabilizer.utils.FileUtils.newFile;
import static com.hazelcast.stabilizer.utils.CommonUtils.exitWithError;

public class AwsProvisionerCli {
    private final static Logger log = Logger.getLogger(AwsProvisionerCli.class);

    public final OptionParser parser = new OptionParser();

    public final OptionSpec<String> makeLB = parser.accepts("newLb",
            "create new load balancer if it dose not exist."
    ).withRequiredArg().ofType(String.class);

    public final OptionSpec<String> agentsToLB = parser.accepts("addToLb",
            "adds the ips in agents.txt file to the load balancer."
    ).withRequiredArg().ofType(String.class);

    public final OptionSpec<Integer> scale = parser.accepts("scale",
            "Desired Number of machines to scale to."
    ).withRequiredArg().ofType(Integer.class);

    public final OptionSpec<String> propertiesFile = parser.accepts("propertiesFile",
            "The file containing the stabilizer properties. If no file is explicitly configured, first the " +
            "working directory is checked for a file 'stabilizer.properties'"
    ).withRequiredArg().ofType(String.class);

    public final OptionSpec help = parser.accepts("help", "Show help").forHelp();


    private AwsProvisioner aws=null;
    private OptionSet options;

    public AwsProvisionerCli(AwsProvisioner awsProvisioner) {
        this.aws = awsProvisioner;
    }

    public void run(String[] args) throws Exception {
        try {
            options = parser.parse(args);
        } catch (OptionException e) {
            exitWithError(log, e.getMessage() + ". Use --help to get overview of the help options.");
        }

        if (options.has(help)) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        if (options.has(propertiesFile)) {
            File f = newFile(options.valueOf(propertiesFile));
            aws.setProperties(f);
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
