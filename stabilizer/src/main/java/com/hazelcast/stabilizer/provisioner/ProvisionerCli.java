package com.hazelcast.stabilizer.provisioner;

import joptsimple.OptionParser;
import joptsimple.OptionSpec;

public class ProvisionerCli {

    public final OptionParser parser = new OptionParser();

    public final OptionSpec restartSpec = parser.accepts("restart",
            "Restarts all agents");

    public final OptionSpec downloadSpec = parser.accepts("download",
            "Download all the files from the workers directory. " +
                    "To delete all worker directories, add the --clean option");

    public final OptionSpec cleanSpec = parser.accepts("clean",
            "Cleans the workers directories. ");

    public final OptionSpec<Integer> scaleSpec = parser.accepts("scale",
            "Number of machines to scale to")
            .withRequiredArg().ofType(Integer.class);

    public final OptionSpec terminateSpec = parser.accepts("terminateWorker",
            "Terminate all members in the provisioner");

    public final OptionSpec killSpec = parser.accepts("kill",
            "Kill all agents");

    public final OptionSpec helpSpec = parser.accepts("help", "Show help").forHelp();
}
