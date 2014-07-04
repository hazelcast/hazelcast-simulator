package com.hazelcast.stabilizer.communicator;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.Utils;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import static com.hazelcast.stabilizer.Utils.getFile;

public class CommunicatorCli {
    private final static ILogger log = Logger.getLogger(CommunicatorCli.class);

    private final Communicator communicator;

    private final OptionParser parser = new OptionParser();
    private OptionSet options;

    private final OptionSpec<String> agentsFileSpec = parser.accepts("agentsFile",
            "The file containing the list of agent machines")
            .withRequiredArg().ofType(String.class).defaultsTo("agents.txt");


    public CommunicatorCli(Communicator communicator) {
        this.communicator = communicator;
    }

    public void init(String[] args) {
        try {
            options = parser.parse(args);
        } catch (OptionException e) {
            Utils.exitWithError(log, e.getMessage() + ". Use --help to get overview of the help options.");
            return;
        }
        communicator.agentsFile = getFile(agentsFileSpec, options, "Agents file");

    }
}
