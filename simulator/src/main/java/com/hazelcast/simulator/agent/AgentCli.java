package com.hazelcast.simulator.agent;

import com.hazelcast.simulator.utils.CliUtils;
import com.hazelcast.simulator.utils.CommandLineExitException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

final class AgentCli {

    private final OptionParser parser = new OptionParser();

    private final OptionSpec<Integer> addressIndexSpec = parser.accepts("addressIndex",
            "Address index of this Agent for the Simulator Communication Protocol.")
            .withRequiredArg().ofType(Integer.class);

    private final OptionSpec<String> publicAddressSpec = parser.accepts("publicAddress",
            "Public address of this Agent.")
            .withRequiredArg().ofType(String.class);

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

    static void init(Agent agent, String[] args) {
        AgentCli agentCli = new AgentCli();
        OptionSet options = CliUtils.initOptionsWithHelp(agentCli.parser, args);

        if (!options.has(agentCli.addressIndexSpec)) {
            throw new CommandLineExitException("Missing parameter: --addressIndex");
        }
        agent.addressIndex = options.valueOf(agentCli.addressIndexSpec);

        if (!options.has(agentCli.publicAddressSpec)) {
            throw new CommandLineExitException("Missing parameter: --publicAddress");
        }
        agent.publicAddress = options.valueOf(agentCli.publicAddressSpec);

        if (options.has(agentCli.cloudProviderSpec)) {
            agent.cloudProvider = options.valueOf(agentCli.cloudProviderSpec);
        }
        if (options.has(agentCli.cloudIdentitySpec)) {
            agent.cloudIdentity = options.valueOf(agentCli.cloudIdentitySpec);
        }
        if (options.has(agentCli.cloudCredentialSpec)) {
            agent.cloudCredential = options.valueOf(agentCli.cloudCredentialSpec);
        }
    }
}
