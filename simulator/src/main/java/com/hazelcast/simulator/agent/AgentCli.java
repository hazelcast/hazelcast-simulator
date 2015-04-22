package com.hazelcast.simulator.agent;

import com.hazelcast.simulator.utils.CliUtils;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

final class AgentCli {

    private final OptionParser parser = new OptionParser();

    private final OptionSpec<String> bindAddress = parser.accepts("bindAddress",
            "Address to bind the agent remote service to.")
            .withRequiredArg().ofType(String.class);

    private final OptionSpec<String> cloudIdentitySpec = parser.accepts("cloudIdentity",
            "Cloud identity")
            .withRequiredArg().ofType(String.class);

    private final OptionSpec<String> cloudCredentialSpec = parser.accepts("cloudCredential",
            "Cloud credential")
            .withRequiredArg().ofType(String.class);

    private final OptionSpec<String> cloudProviderSpec = parser.accepts("cloudProvider",
            "Cloud provider")
            .withRequiredArg().ofType(String.class);

    private AgentCli() {
    }

    static void init(Agent agent, String[] args) {
        AgentCli agentCli = new AgentCli();
        OptionSet options = CliUtils.initOptionsWithHelp(agentCli.parser, args);

        if (options.has(agentCli.bindAddress)) {
            agent.bindAddress = options.valueOf(agentCli.bindAddress);
        }

        if (options.has(agentCli.cloudIdentitySpec)) {
            agent.cloudIdentity = options.valueOf(agentCli.cloudIdentitySpec);
        }

        if (options.has(agentCli.cloudCredentialSpec)) {
            agent.cloudCredential = options.valueOf(agentCli.cloudCredentialSpec);
        }

        if (options.has(agentCli.cloudProviderSpec)) {
            agent.cloudProvider = options.valueOf(agentCli.cloudProviderSpec);
        }
    }
}
