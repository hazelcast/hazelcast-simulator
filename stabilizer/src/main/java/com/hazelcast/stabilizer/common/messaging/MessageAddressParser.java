package com.hazelcast.stabilizer.common.messaging;

public class MessageAddressParser {
    /**
     * Warning: Change user help in {@link com.hazelcast.stabilizer.communicator.CommunicatorCli} when changing
     * input format accepted by this parser
     *
     */
    public static final String AGENT = "Agent";
    public static final String WORKER = "Worker";
    public static final String TEST = "Test";

    public static final String BROADCAST_MODE = "*";
    public static final String RANDOM_MODE = "R";
    public static final String OLDEST_MEMBER = "O";

    public static final String ADDRESS_SEPARATOR = ",";

    public MessageAddress parse(final String originInput) {
        String input = originInput;
        if (input == null) {
            throw new IllegalArgumentException("Input string cannot be null");
        }


        String prefix = AGENT+"=";
        String agentAddress = getAddress(input, prefix);
        input = input.substring(prefix.length() + 1);
        if (input.isEmpty()) {
            return new MessageAddress(agentAddress, null, null);
        }
        input = skipSeparator(input);

        prefix = WORKER+"=";
        String workerAddress = getAddress(input, prefix);
        input = input.substring(prefix.length() + 1);
        if (input.isEmpty()) {
            return new MessageAddress(agentAddress, workerAddress, null);
        }
        input = skipSeparator(input);

        prefix = TEST+"=";
        String testAddress = getAddress(input, prefix);
        input = input.substring(prefix.length() + 1);
        if (input.isEmpty()) {
            return new MessageAddress(agentAddress, workerAddress, testAddress);
        }
        throw new IllegalArgumentException("Address too long, found: '"+originInput+"'");
    }

    private String skipSeparator(String input) {
        String separator = input.substring(0, 1);
        if (!ADDRESS_SEPARATOR.equals(separator)) {
            throw new IllegalArgumentException(
                    String.format("Wrong address separator. %nExpected: '%s'%n Found '%s'.", ADDRESS_SEPARATOR, separator)
            );
        }
        return input.substring(1);
    }

    private String getAddress(String input, String prefix) {
        String address;
        int index = input.indexOf(prefix);
        if (index != 0) {
            throw wrongFormat(input);
        }
        if (input.length() == prefix.length()) {
            throw wrongFormat(input);
        }

        input = input.substring(prefix.length());
        String mode = input.substring(0, 1);
        if (BROADCAST_MODE.equals(mode)) {
            address = MessageAddress.BROADCAST_PREFIX;
        } else if (RANDOM_MODE.equals(mode)) {
            address = MessageAddress.RANDOM_PREFIX;
        } else if (OLDEST_MEMBER.equals(mode)) {
            address = MessageAddress.OLDEST_MEMBER_PREFIX;
        } else {
            throw unknownAddressMode(mode);
        }
        return address;
    }

    private RuntimeException unknownAddressMode(String agentMode) {
        return new IllegalArgumentException(
                String.format("Unknown address mode '%s', known address modes: %s, %s, %s",
                        agentMode, BROADCAST_MODE, RANDOM_MODE, OLDEST_MEMBER)
        );
    }

    private RuntimeException wrongFormat(String address) {
        return new IllegalArgumentException(
                String.format("Address has to be in a format Syntax: %s=<%s,%s>[,%s=<%s,%s>[,%s=<%s, %s, %s>]], found: '%s'",
                        AGENT, BROADCAST_MODE, RANDOM_MODE, WORKER, BROADCAST_MODE, RANDOM_MODE, TEST,
                        BROADCAST_MODE, RANDOM_MODE,OLDEST_MEMBER,
                        address));
    }
}
