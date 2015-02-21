package com.hazelcast.stabilizer.communicator;

import com.hazelcast.stabilizer.common.messaging.Message;
import com.hazelcast.stabilizer.common.messaging.MessageAddress;
import com.hazelcast.stabilizer.common.messaging.MessageAddressParser;
import joptsimple.BuiltinHelpFormatter;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

import static com.hazelcast.stabilizer.utils.CommonUtils.exitWithError;
import static com.hazelcast.stabilizer.utils.FileUtils.getFile;

public class CommunicatorCli {
    private final static Logger log = Logger.getLogger(CommunicatorCli.class);

    private final Communicator communicator;

    private final OptionParser parser = new OptionParser();
    private OptionSet options;

    private final OptionSpec<String> agentsFileSpec = parser.accepts("agentsFile",
            "The file containing the list of agent machines")
            .withRequiredArg().ofType(String.class).defaultsTo("agents.txt");

    private final OptionSpec<String> messageTypeSpec = parser.accepts("message-type",
            String.format("Message type definition. Supported message types: %n%s", Message.getMessageHelp()))
            .withRequiredArg().ofType(String.class);

    private final OptionSpec<String> messageAddressSpec = parser.accepts("message-address",
            String.format("Message address definition. %nSyntax: Agent=<mode>[,Worker=<mode>[,Test=<mode>]]. " +
                    "Mode can be either '%s' for broadcast' or '%s' for a single random destination. %n" +
                    "Examples: %n--message-address 'Agent=*,Worker=R' - a message will be routed to all agents and than " +
                    "each agent will pass it to a single random worker for processing. %n" +
                    "          %n--message-address 'Agent=R,Worker=R,Test=*' - a message will be router to a single random " +
                    "agent. The agent will pass it to a single random worker and the worker will pass the message to all " +
                    "tests.", MessageAddressParser.ALL_WORKERS, MessageAddressParser.RANDOM_WORKER))
            .withRequiredArg().ofType(String.class);

    private final OptionSpec oldestMemberSpec = parser.accepts("oldest-member",
            "Send the message to a worker with the oldest cluster member.");

    private final OptionSpec randomAgentSpec = parser.accepts("random-agent",
            "Send the message to a random agent. Cannot be used together with --message-address or any other addressing option.");

    private final OptionSpec randomWorkerSpec = parser.accepts("random-worker",
            "Send the message to a worker agent. Cannot be used together with --message-address or any other addressing option.");

    private final OptionSpec helpSpec = parser.accepts("help", "Show help").forHelp();

    public CommunicatorCli(Communicator communicator) {
        this.communicator = communicator;
    }

    public void init(String[] args) throws IOException {
        try {
            options = parser.parse(args);
        } catch (OptionException e) {
            exitWithError(log, e.getMessage() + ". Use --help to get overview of the help options.");
            return;
        }
        if (options.has(helpSpec)) {
            parser.formatHelpWith(new BuiltinHelpFormatter(160, 2));
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        String messageTypeString = null;
        List<String> noArgOptions = options.nonOptionArguments();
        if (options.has(messageTypeSpec)) {
            if (!noArgOptions.isEmpty()) {
                exitWithError(log, "You cannot use --message-type simultaneously with a message shortcut. "
                        + "Use --help to get overview of the help options.");
            }
            messageTypeString = options.valueOf(this.messageTypeSpec);
        }else if (!noArgOptions.isEmpty()) {
            if (noArgOptions.size() > 1) {
                exitWithError(log, "You cannot use more than 1 message shortcut.");
            }
            messageTypeString = noArgOptions.get(0);
        } else  {
            exitWithError(log, "You have to use either --message-type or message shortcut "
                    + "Use --help to get overview of the help options.");
        }

        MessageAddress messageAddress = null;
        if (options.has(randomAgentSpec)) {
            checkHasOnlyAddressingOption(randomAgentSpec);
            messageAddress = MessageAddress.builder().toRandomAgent().build();
        } else if (options.has(oldestMemberSpec)) {
            checkHasOnlyAddressingOption(oldestMemberSpec);
            messageAddress = MessageAddress.builder().toOldestMember().build();
        } else if (options.has(messageAddressSpec)) {
            checkHasOnlyAddressingOption(messageAddressSpec);
            MessageAddressParser addressParser = new MessageAddressParser();
            String messageAddressString = options.valueOf(messageAddressSpec);
            messageAddress = addressParser.parse(messageAddressString);
        } else if (options.has(randomWorkerSpec)) {
            checkHasOnlyAddressingOption(randomWorkerSpec);
            messageAddress = MessageAddress.builder().toAllAgents().toRandomWorker().build();
        } else {
            exitWithError(log, "You have to use either --oldest-member or --message-address to specify message address"
                            + ". Use --help to get overview of the help options.");
        }
        communicator.message = Message.newBySpec(messageTypeString, messageAddress);
        communicator.agentsFile = getFile(agentsFileSpec, options, "Agents file");
    }

    private void checkHasOnlyAddressingOption(OptionSpec optionSpec) {
        if (hasOtherAddressOptionThen(optionSpec)) {
            exitWithError(log, "You cannot use --random-agent and --message-address or any other addressing option "
                    + "simultaneously. Use --help to get overview of the help options.");
        }
    }

    private boolean hasOtherAddressOptionThen(OptionSpec optionSpec) {
        OptionSpec[] addressOptionSpecs = new OptionSpec[]{
                messageAddressSpec,
                randomAgentSpec,
                oldestMemberSpec,
                randomWorkerSpec};

        for (OptionSpec o : addressOptionSpecs) {
            if (!o.equals(optionSpec) && options.has(o)) {
                return true;
            }
        }
        return false;
    }
}
