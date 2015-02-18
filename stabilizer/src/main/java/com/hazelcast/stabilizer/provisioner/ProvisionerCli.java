package com.hazelcast.stabilizer.provisioner;

import com.hazelcast.stabilizer.Utils;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.log4j.Logger;

import java.io.File;

public class ProvisionerCli {
    private final static Logger log = Logger.getLogger(ProvisionerCli.class);

    public final OptionParser parser = new OptionParser();

    private final OptionSpec<String> gitSpec = parser.accepts("git",
            "Overrides the HAZELCAST_VERSION_SPEC property and forces Provisioner to build " +
                    "Hazelcast JARs from a given GIT version. This makes it easier to run a test " +
                    "with different versions of Hazelcast. \n " +
                    "E.g. --git f0288f713                to use the Git revision f0288f713 \n" +
                    "     --git myRepository/myBranch    to use branch myBranch from a repository myRepository. " +
                    "You can specify custom repositories in stabilizer.properties.")
            .withRequiredArg().ofType(String.class);

    public final OptionSpec restartSpec = parser.accepts("restart",
            "Restarts all agents");

    public final OptionSpec<String> downloadSpec = parser.accepts("download",
            "Download all the files from the workers directory. " +
            "To delete all worker directories, run with --clean"
    ).withOptionalArg().defaultsTo("workers").ofType(String.class);

    public final OptionSpec cleanSpec = parser.accepts("clean",
            "Cleans the workers directories.");


    public final OptionSpec<String> makeLBSpec = parser.accepts("makeLB",
            "takes the name of a load balancer to create.  Makes an AWS load balancer with a given name, if it dose not exist (AWS only)."
    ).withRequiredArg().ofType(String.class);

    public final OptionSpec<String> agentsToLB = parser.accepts("agentsToLB",
            "given the name of a load balancer adds the ips in Agents.txt file to the named balancer (AWS only)."
    ).withRequiredArg().ofType(String.class);

    public final OptionSpec<String> withKeySpec = parser.accepts("awsWithKey",
            "find aws instances with key name, if jclouds times but instances are created, this utils helps to reclame them (aws only)."
    ).withRequiredArg().ofType(String.class);;


    public final OptionSpec<Integer> scaleSpec = parser.accepts("scale",
            "Number of agent machines to scale to. If the number of machines already exists, the call is ignored. If the " +
                    "desired number of machines is smaller than the actual number of machines, machines are terminated."
    )
            .withRequiredArg().ofType(Integer.class);

    public final OptionSpec terminateSpec = parser.accepts("terminate",
            "Terminate all agent machines in the provisioner");

    public final OptionSpec killSpec = parser.accepts("kill",
            "Kill all the agent processes (it will do a killall -9 java and kills all java processes)");

    public final OptionSpec listAgentsSpec = parser.accepts("listAgents", "Lists the running agents.");

    public final OptionSpec helpSpec = parser.accepts("help", "Show help").forHelp();

    private final OptionSpec<String> propertiesFileSpec = parser.accepts("propertiesFile",
            "The file containing the stabilizer properties. If no file is explicitly configured, first the " +
                    "working directory is checked for a file 'stabilizer.properties'. All missing properties" +
                    "are always loaded from STABILIZER_HOME/conf/stabilizer.properties"
    ).withRequiredArg().ofType(String.class);

    private final OptionSpec<Boolean> enterpriseEnabledSpec = parser.accepts("enterpriseEnabled",
            "Use hazelcast enterprise edition JARs.")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(false);

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
        }else if (options.has(makeLBSpec)) {
            String name = options.valueOf(makeLBSpec);
            AwsProvisioner aws = new AwsProvisioner();
            aws.createLoadBalancer(name);
        }else if (options.has(agentsToLB)) {
            String name = options.valueOf(agentsToLB);
            AwsProvisioner aws = new AwsProvisioner();
            aws.addAgentsToLoadBalancer(name);
        }else if (options.has(withKeySpec)) {
            String name = options.valueOf(withKeySpec);
            AwsProvisioner aws = new AwsProvisioner();
            aws.findAwsInstanceWithKeyName(name);
        } else {
            parser.printHelpOn(System.out);
        }
    }

    private File getPropertiesFile() {
        if (options.has(propertiesFileSpec)) {
            //a file was explicitly configured
            return Utils.newFile(options.valueOf(propertiesFileSpec));
        } else {
            return null;
        }
    }
}
