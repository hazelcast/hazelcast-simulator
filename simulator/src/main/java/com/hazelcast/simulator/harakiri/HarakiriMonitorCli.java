/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.harakiri;

import com.hazelcast.simulator.utils.CliUtils;
import com.hazelcast.simulator.utils.CommandLineExitException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;

@SuppressWarnings({"checkstyle:hideutilityclassconstructor", "FieldCanBeLocal"})
final class HarakiriMonitorCli {

    private static final Logger LOGGER = Logger.getLogger(HarakiriMonitorCli.class);

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
    private final HarakiriMonitor harakiriMonitor;

    HarakiriMonitorCli(String[] args) {
        OptionSet options = CliUtils.initOptionsWithHelp(parser, args);

        if (!options.has(cloudProviderSpec)) {
            throw new CommandLineExitException("You have to provide --cloudProvider");
        }
        if (!options.has(cloudIdentitySpec)) {
            throw new CommandLineExitException("You have to provide --cloudIdentity");
        }
        if (!options.has(cloudCredentialSpec)) {
            throw new CommandLineExitException("You have to provide --cloudCredential");
        }

        harakiriMonitor = new HarakiriMonitor(
                options.valueOf(cloudProviderSpec),
                options.valueOf(cloudIdentitySpec),
                options.valueOf(cloudCredentialSpec),
                options.valueOf(timeoutSpec));
    }

    private void run() {
        harakiriMonitor.start();
    }

    public static void main(String[] args) {
        try {
            HarakiriMonitorCli cli = new HarakiriMonitorCli(args);
            cli.run();
        } catch (Exception e) {
            exitWithError(LOGGER, "Could not start HarakiriMonitor!", e);
        }
    }
}
