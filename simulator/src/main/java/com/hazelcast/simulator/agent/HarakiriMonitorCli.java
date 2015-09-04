package com.hazelcast.simulator.agent;

import com.hazelcast.simulator.utils.CliUtils;
import com.hazelcast.simulator.utils.CommandLineExitException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.util.concurrent.TimeUnit;

final class HarakiriMonitorCli {

    private final OptionParser parser = new OptionParser();

    private final OptionSpec<String> cloudProviderSpec = parser.accepts("cloudProvider",
            "Cloud provider")
            .withRequiredArg().ofType(String.class);

    private final OptionSpec<String> cloudIdentitySpec = parser.accepts("cloudIdentity",
            "Cloud identity")
            .withRequiredArg().ofType(String.class);

    private final OptionSpec<String> cloudCredentialSpec = parser.accepts("cloudCredential",
            "Cloud credential")
            .withRequiredArg().ofType(String.class);

    private final OptionSpec<Integer> timeoutSpec = parser.accepts("waitSeconds",
            "The number of seconds the HarakiriMonitor will wait until it kills the machine.")
            .withRequiredArg().ofType(Integer.class).defaultsTo((int) TimeUnit.HOURS.toSeconds(2));

    private HarakiriMonitorCli() {
    }

    static HarakiriMonitor createHarakiriMonitor(String[] args) {
        HarakiriMonitorCli harakiriCli = new HarakiriMonitorCli();
        OptionSet options = CliUtils.initOptionsWithHelp(harakiriCli.parser, args);

        if (!options.has(harakiriCli.cloudProviderSpec)) {
            throw new CommandLineExitException("You have to provide --cloudProvider");
        }
        if (!options.has(harakiriCli.cloudIdentitySpec)) {
            throw new CommandLineExitException("You have to provide --cloudIdentity");
        }
        if (!options.has(harakiriCli.cloudCredentialSpec)) {
            throw new CommandLineExitException("You have to provide --cloudCredential");
        }

        return new HarakiriMonitor(
                options.valueOf(harakiriCli.cloudProviderSpec),
                options.valueOf(harakiriCli.cloudIdentitySpec),
                options.valueOf(harakiriCli.cloudCredentialSpec),
                options.valueOf(harakiriCli.timeoutSpec));
    }
}
