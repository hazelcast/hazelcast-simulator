package com.hazelcast.simulator.common.messaging;

import java.util.EnumSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MessageAddressParser {
    /**
     * Warning: Change user help in {@link com.hazelcast.simulator.communicator.CommunicatorCli#messageAddressSpec}
     * when changing input format accepted by this parser
     */
    public static final String AGENT = "Agent";
    public static final String WORKER = "Worker";
    public static final String TEST = "Test";

    public static final String ALL_WORKERS = "*";
    public static final String WORKERS_WITH_MEMBER = "*m";
    public static final String RANDOM_WORKER = "R";
    public static final String RANDOM_WORKER_WITH_MEMBER = "Rm";
    public static final String OLDEST_MEMBER = "O";

    public static final String ADDRESS_SEPARATOR = ",";

    private enum ParserState {
        START,
        BEFORE_AGENT_ADDRESS,
        AFTER_AGENT_ADDRESS,
        BEFORE_WORKER_ADDRESS,
        AFTER_WORKER_ADDRESS,
        BEFORE_TEST_ADDRESS,
        AFTER_TEST_ADDRESS,
        DONE
    }

    public MessageAddress parse(String input) {
        if (input == null) {
            throw new IllegalArgumentException("Input string cannot be null");
        }
        Pattern pattern = Pattern.compile("(Agent=)(\\*|R)(,Worker=)?(\\*|R|O|\\*m|Rm)?(,Test=)?(\\*|R)?");
        Matcher matcher = pattern.matcher(input);

        MessageAddress.MessageAddressBuilder builder = MessageAddress.builder();
        ParserState state = ParserState.START;
        if (!matcher.matches()) {
            throw wrongFormat(input);
        }
        for (int i = 1; i <= matcher.groupCount(); i++) {
            String group = matcher.group(i);
            if (group == null) {
                if (EnumSet.of(ParserState.AFTER_AGENT_ADDRESS, ParserState.AFTER_WORKER_ADDRESS).contains(state)) {
                    state = ParserState.DONE;
                    break;
                } else {
                    throw wrongFormat(input);
                }
            }
            if (state.equals(ParserState.START)) {
                if ("Agent=".equals(group)) {
                    state = ParserState.BEFORE_AGENT_ADDRESS;
                } else {
                    throw wrongFormat(input);
                }
            } else if (state.equals(ParserState.BEFORE_AGENT_ADDRESS)) {
                if (group.equals("*")) {
                    builder.toAllAgents();
                } else if (group.equals("R")) {
                    builder.toRandomAgent();
                } else {
                    throw wrongFormat(input);
                }
                state = ParserState.AFTER_AGENT_ADDRESS;
            } else if (state.equals(ParserState.AFTER_AGENT_ADDRESS)) {
                if (group.equals(",Worker=")) {
                    state = ParserState.BEFORE_WORKER_ADDRESS;
                } else {
                    throw wrongFormat(input);
                }
            } else if (state.equals(ParserState.BEFORE_WORKER_ADDRESS)) {
                if (group.equals("*")) {
                    builder.toAllWorkers();
                } else if (group.equals("R")) {
                    builder.toRandomWorker();
                } else if (group.equals("O")) {
                    builder.toOldestMember();
                } else if (group.equals("*m")) {
                    builder.toWorkersWithClusterMember();
                } else if (group.equals("Rm")) {
                    builder.toRandomWorkerWithMember();
                } else {
                    throw wrongFormat(input);
                }
                state = ParserState.AFTER_WORKER_ADDRESS;
            } else if (state.equals(ParserState.AFTER_WORKER_ADDRESS)) {
                if (group.equals(",Test=")) {
                    state = ParserState.BEFORE_TEST_ADDRESS;
                } else {
                    throw wrongFormat(input);
                }
            } else if (state.equals(ParserState.BEFORE_TEST_ADDRESS)) {
                if (group.equals("*")) {
                    builder.toAllTests();
                } else if (group.equals("R")) {
                    builder.toRandomTest();
                } else {
                    throw wrongFormat(input);
                }
                state = ParserState.DONE;
            } else {
                throw wrongFormat(input);
            }
        }
        if (!ParserState.DONE.equals(state)) {
            throw wrongFormat(input);
        }
        return builder.build();
    }


    private RuntimeException wrongFormat(String address) {
        return new IllegalArgumentException(
                String.format("Address '%s' has a wrong format. Please use communicator --help to see the syntax",
                        address));
    }

}
