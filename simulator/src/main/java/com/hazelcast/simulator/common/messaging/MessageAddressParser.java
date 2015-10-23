/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.common.messaging;

import com.hazelcast.simulator.common.messaging.MessageAddress.MessageAddressBuilder;

import java.util.EnumSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.util.regex.Pattern.compile;

/**
 * Parses the {@link MessageAddress} from the command line argument.
 *
 * Warning: Change user help in CommunicatorCli when changing input format accepted by this parser.
 */
public class MessageAddressParser {

    public static final String AGENT = "Agent";
    public static final String WORKER = "Worker";
    public static final String TEST = "Test";

    public static final String ALL = "*";
    public static final String RANDOM = "R";

    public static final String OLDEST_MEMBER = "O";
    public static final String WORKERS_WITH_MEMBER = "*m";
    public static final String RANDOM_WORKER_WITH_MEMBER = "Rm";

    private static final Pattern PATTERN = compile("(Agent=)(\\*|R)(,Worker=)?(\\*|R|O|\\*m|Rm)?(,Test=)?(\\*|R)?");

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

        Matcher matcher = PATTERN.matcher(input);
        ParserState state = ParserState.START;
        MessageAddressBuilder builder = MessageAddress.builder();
        if (!matcher.matches()) {
            throw wrongFormat(input);
        }
        for (int i = 1; i <= matcher.groupCount(); i++) {
            String group = matcher.group(i);
            if (group == null) {
                if (EnumSet.of(ParserState.AFTER_AGENT_ADDRESS, ParserState.AFTER_WORKER_ADDRESS).contains(state)) {
                    state = ParserState.DONE;
                    break;
                }
                throw wrongFormat(input);
            }
            state = parseState(state, input, group, builder);
        }
        if (!ParserState.DONE.equals(state)) {
            throw wrongFormat(input);
        }
        return builder.build();
    }

    private ParserState parseState(ParserState state, String input, String group, MessageAddressBuilder builder) {
        if (state.equals(ParserState.START)) {
            return parseStart(input, group);
        } else if (state.equals(ParserState.BEFORE_AGENT_ADDRESS)) {
            return parseBeforeAgentAddress(input, builder, group);
        } else if (state.equals(ParserState.AFTER_AGENT_ADDRESS)) {
            return parseAfterAgentAddress(input, group);
        } else if (state.equals(ParserState.BEFORE_WORKER_ADDRESS)) {
            return parseBeforeWorkerAddress(input, builder, group);
        } else if (state.equals(ParserState.AFTER_WORKER_ADDRESS)) {
            return parseAfterWorkerAddress(input, group);
        } else if (state.equals(ParserState.BEFORE_TEST_ADDRESS)) {
            return parseBeforeTestAddress(input, builder, group);
        }
        throw wrongFormat(input);
    }

    private ParserState parseStart(String input, String group) {
        if ("Agent=".equals(group)) {
            return ParserState.BEFORE_AGENT_ADDRESS;
        }
        throw wrongFormat(input);
    }

    private ParserState parseBeforeAgentAddress(String input, MessageAddressBuilder builder, String group) {
        if (group.equals(ALL)) {
            builder.toAllAgents();
        } else if (group.equals(RANDOM)) {
            builder.toRandomAgent();
        } else {
            throw wrongFormat(input);
        }
        return ParserState.AFTER_AGENT_ADDRESS;
    }

    private ParserState parseAfterAgentAddress(String input, String group) {
        if (group.equals(",Worker=")) {
            return ParserState.BEFORE_WORKER_ADDRESS;
        }
        throw wrongFormat(input);
    }

    private ParserState parseBeforeWorkerAddress(String input, MessageAddressBuilder builder, String group) {
        if (group.equals(ALL)) {
            builder.toAllWorkers();
        } else if (group.equals(RANDOM)) {
            builder.toRandomWorker();
        } else if (group.equals(OLDEST_MEMBER)) {
            builder.toOldestMember();
        } else if (group.equals(WORKERS_WITH_MEMBER)) {
            builder.toWorkersWithClusterMember();
        } else if (group.equals(RANDOM_WORKER_WITH_MEMBER)) {
            builder.toRandomWorkerWithMember();
        } else {
            throw wrongFormat(input);
        }
        return ParserState.AFTER_WORKER_ADDRESS;
    }

    private ParserState parseAfterWorkerAddress(String input, String group) {
        if (group.equals(",Test=")) {
            return ParserState.BEFORE_TEST_ADDRESS;
        }
        throw wrongFormat(input);
    }

    private ParserState parseBeforeTestAddress(String input, MessageAddressBuilder builder, String group) {
        if (group.equals(ALL)) {
            builder.toAllTests();
        } else if (group.equals(RANDOM)) {
            builder.toRandomTest();
        } else {
            throw wrongFormat(input);
        }
        return ParserState.DONE;
    }

    private RuntimeException wrongFormat(String address) {
        return new IllegalArgumentException(
                format("Address '%s' has a wrong format. Please use communicator --help to see the syntax", address));
    }
}
