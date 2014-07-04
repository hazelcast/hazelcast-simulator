package com.hazelcast.stabilizer.communicator;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.common.messaging.MessageAddress;
import com.hazelcast.stabilizer.common.messaging.MessageAddressParser;
import joptsimple.BuiltinHelpFormatter;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.IOException;

import static com.hazelcast.stabilizer.Utils.getFile;

public class CommunicatorCli {
    private final static ILogger log = Logger.getLogger(CommunicatorCli.class);

    private final Communicator communicator;

    private final OptionParser parser = new OptionParser();
    private OptionSet options;

    private final OptionSpec<String> agentsFileSpec = parser.accepts("agentsFile",
            "The file containing the list of agent machines")
            .withRequiredArg().ofType(String.class).defaultsTo("agents.txt");

    private final OptionSpec<String> messageTypeSpec = parser.accepts("messageType",
            "Message type definition. ")
            .withRequiredArg().ofType(String.class).required();

    private final OptionSpec<String> messageAddressSpec = parser.accepts("messageAddress",
            String.format("Message address definition. %nSyntax: Agent=<mode>[,Worker=<mode>[,Test=<mode>]]. " +
                    "Mode can be either '%s' for broadcast' or '%s' for a single random destination. %n" +
                    "Examples: %n--mesageAddress 'Agent=*,Worker=R' - a message will be routed to all agents and than each agent " +
                    "will pass it to a single random worker for processing. %n" +
                    "          %n--mesageAddress 'Agent=R,Worker=R,Test=*' - a message will be router to a single random " +
                    "agent. The agent will pass it to a single random worker and the worker will pass the message to all " +
                    "tests.", MessageAddressParser.BROADCAST_MODE, MessageAddressParser.RANDOM_MODE))
            .withRequiredArg().ofType(String.class).required();

    private final OptionSpec helpSpec = parser.accepts("help", "Show help").forHelp();


    public CommunicatorCli(Communicator communicator) {
        this.communicator = communicator;
    }

    public void init(String[] args) throws IOException {
        try {
            options = parser.parse(args);
        } catch (OptionException e) {
            Utils.exitWithError(log, e.getMessage() + ". Use --help to get overview of the help options.");
            return;
        }
        if (options.has(helpSpec)) {
            parser.formatHelpWith(new BuiltinHelpFormatter(160, 2));
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        communicator.agentsFile = getFile(agentsFileSpec, options, "Agents file");

    }
}
