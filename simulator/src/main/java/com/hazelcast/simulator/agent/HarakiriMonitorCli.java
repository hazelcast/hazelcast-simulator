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
