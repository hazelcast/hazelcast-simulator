/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.agent;

import com.hazelcast.simulator.utils.CliUtils;
import com.hazelcast.simulator.utils.CommandLineExitException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

final class AgentCli {

    private static final int DEFAULT_WORKER_LAST_SEEN_TIMEOUT_SECONDS = 180;

    private final OptionParser parser = new OptionParser();

    private final OptionSpec<Integer> addressIndexSpec = parser.accepts("addressIndex",
            "Address index of this Agent for the Simulator Communication Protocol.")
            .withRequiredArg().ofType(Integer.class);

    private final OptionSpec<String> publicAddressSpec = parser.accepts("publicAddress",
            "Public address of this Agent.")
            .withRequiredArg().ofType(String.class);

    private final OptionSpec<Integer> portSpec = parser.accepts("port",
            "Port of this Agent.")
            .withRequiredArg().ofType(Integer.class);

    private final OptionSpec<Integer> threadPoolSizeSpec = parser.accepts("threadPoolSize",
            "Size of the thread pool to connect to Worker instances.")
            .withRequiredArg().ofType(Integer.class).defaultsTo(0);

    private final OptionSpec<Integer> workerLastSeenTimeoutSecondsSpec = parser.accepts("workerLastSeenTimeoutSeconds",
            "Timeout value for worker timeout detection.")
            .withRequiredArg().ofType(Integer.class).defaultsTo(DEFAULT_WORKER_LAST_SEEN_TIMEOUT_SECONDS);

    private final OptionSpec<String> cloudProviderSpec = parser.accepts("cloudProvider",
            "The cloud provider for this Agent.")
            .withRequiredArg().ofType(String.class);

    private final OptionSpec<String> cloudIdentitySpec = parser.accepts("cloudIdentity",
            "The cloud identity for this Agent.")
            .withRequiredArg().ofType(String.class);

    private final OptionSpec<String> cloudCredentialSpec = parser.accepts("cloudCredential",
            "The cloud credential for this Agent.")
            .withRequiredArg().ofType(String.class);

    private AgentCli() {
    }

    static Agent init(String[] args) {
        AgentCli agentCli = new AgentCli();
        OptionSet options = CliUtils.initOptionsWithHelp(agentCli.parser, args);

        if (!options.has(agentCli.addressIndexSpec)) {
            throw new CommandLineExitException("Missing parameter: --addressIndex");
        }
        int addressIndex = options.valueOf(agentCli.addressIndexSpec);

        if (!options.has(agentCli.publicAddressSpec)) {
            throw new CommandLineExitException("Missing parameter: --publicAddress");
        }
        String publicAddress = options.valueOf(agentCli.publicAddressSpec);

        if (!options.has(agentCli.portSpec)) {
            throw new CommandLineExitException("Missing parameter: --port");
        }
        int port = options.valueOf(agentCli.portSpec);

        String cloudProvider = options.valueOf(agentCli.cloudProviderSpec);
        String cloudIdentity = options.valueOf(agentCli.cloudIdentitySpec);
        String cloudCredential = options.valueOf(agentCli.cloudCredentialSpec);
        Integer threadPoolSize = options.valueOf(agentCli.threadPoolSizeSpec);
        Integer workerLastSeenTimeoutSeconds = options.valueOf(agentCli.workerLastSeenTimeoutSecondsSpec);

        return new Agent(addressIndex, publicAddress, port, cloudProvider, cloudIdentity, cloudCredential, threadPoolSize,
                workerLastSeenTimeoutSeconds);
    }
}
