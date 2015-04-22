package com.hazelcast.simulator.communicator;

import com.hazelcast.simulator.common.AgentsFile;
import com.hazelcast.simulator.common.messaging.Message;
import com.hazelcast.simulator.common.messaging.MessageAddress;
import com.hazelcast.simulator.common.messaging.MessageAddressParser;
import com.hazelcast.simulator.utils.CommandLineExitException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.util.List;

import static com.hazelcast.simulator.utils.CliUtils.initOptionsWithHelp;
import static com.hazelcast.simulator.utils.FileUtils.getFile;

public class CommunicatorCli {

    private static final String HELP_ADVICE = " Use --help to get overview of the help options.";

    private final OptionParser parser = new OptionParser();

    private final OptionSpec<String> agentsFileSpec = parser.accepts("agentsFile",
            "The file containing the list of agent machines")
            .withRequiredArg().ofType(String.class).defaultsTo(AgentsFile.NAME);

    private final OptionSpec<String> messageTypeSpec = parser.accepts("message-type",
            String.format("Message type definition. Supported message types:%n%s", Message.getMessageHelp()))
            .withRequiredArg().ofType(String.class);

    private final OptionSpec<String> messageAddressSpec = parser.accepts("message-address",
            String.format("Message address definition.%nSyntax: %s=<mode>[,%s=<mode>[,%s=<mode>]]."
                            + " Mode can be either '%s' for broadcast' or '%s' for a single random destination.%nExamples:"
                            + "%n--message-address 'Agent=*,Worker=R' - a message will be routed to all agents and then each agent"
                            + " will pass it to a single random worker for processing."
                            + "%n--message-address 'Agent=R,Worker=R,Test=*' - a message will be routed to a single random agent."
                            + " The agent will pass it to a single random worker and the worker will pass the message to all tests.",
                    MessageAddressParser.AGENT, MessageAddressParser.WORKER, MessageAddressParser.TEST,
                    MessageAddressParser.ALL, MessageAddressParser.RANDOM))
            .withRequiredArg().ofType(String.class);

    private final OptionSpec oldestMemberSpec = parser.accepts("oldest-member",
            "Send the message to a worker with the oldest cluster member.");

    private final OptionSpec randomAgentSpec = parser.accepts("random-agent",
            "Send the message to a random agent. Cannot be used together with --message-address or any other addressing option.");

    private final OptionSpec randomWorkerSpec = parser.accepts("random-worker",
            "Send the message to a worker agent. Cannot be used together with --message-address or any other addressing option.");

    private final Communicator communicator;
    private final OptionSet options;

    public CommunicatorCli(Communicator communicator, String[] args) {
        this.communicator = communicator;
        this.options = initOptionsWithHelp(parser, args);
    }

    public void init() {
        String messageTypeString;
        List<String> noArgOptions = options.nonOptionArguments();
        if (options.has(messageTypeSpec)) {
            if (!noArgOptions.isEmpty()) {
                throw new CommandLineExitException("You cannot use --message-type simultaneously with a message shortcut."
                        + HELP_ADVICE);
            }
            messageTypeString = options.valueOf(this.messageTypeSpec);
        } else if (!noArgOptions.isEmpty()) {
            if (noArgOptions.size() > 1) {
                throw new CommandLineExitException("You cannot use more than one message shortcut at a time." + HELP_ADVICE);
            }
            messageTypeString = noArgOptions.get(0);
        } else {
            throw new CommandLineExitException("You have to use either --message-type or message shortcut." + HELP_ADVICE);
        }

        MessageAddress messageAddress;
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
            throw new CommandLineExitException("You have to use either --oldest-member or --message-address to specify message"
                    + " address." + HELP_ADVICE);
        }

        communicator.message = Message.newBySpec(messageTypeString, messageAddress);
        communicator.agentsFile = getFile(agentsFileSpec, options, "Agents file");
    }

    private void checkHasOnlyAddressingOption(OptionSpec optionSpec) {
        if (hasOtherAddressOptionThen(optionSpec)) {
            throw new CommandLineExitException("You cannot use --random-agent and --message-address or any other addressing"
                    + " option simultaneously." + HELP_ADVICE);
        }
    }

    private boolean hasOtherAddressOptionThen(OptionSpec optionSpec) {
        OptionSpec[] addressOptionSpecs = new OptionSpec[]{
                messageAddressSpec,
                randomAgentSpec,
                oldestMemberSpec,
                randomWorkerSpec,
        };

        for (OptionSpec option : addressOptionSpecs) {
            if (!option.equals(optionSpec) && options.has(option)) {
                return true;
            }
        }
        return false;
    }
}
