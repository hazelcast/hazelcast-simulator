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
package com.hazelcast.simulator.wizard;

import com.hazelcast.simulator.common.AgentsFile;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.utils.Bash;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import static com.hazelcast.simulator.utils.CliUtils.initOptionsWithHelp;
import static com.hazelcast.simulator.utils.CliUtils.printHelpAndExit;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_LOCAL;
import static com.hazelcast.simulator.wizard.WizardUtils.getProfileFile;
import static com.hazelcast.simulator.wizard.WizardUtils.getSimulatorPath;
import static java.lang.String.format;

final class WizardCli {

    private final OptionParser parser = new OptionParser();

    private final OptionSpec installSpec = parser.accepts("install",
            "Installs Hazelcast Simulator on the local machine.");

    private final OptionSpec<String> createWorkDirSpec = parser.accepts("createWorkDir",
            "Creates a working directory with the given name."
                    + " You can specify a cloud provider with --cloudProvider to customize the setup.")
            .withOptionalArg().ofType(String.class).defaultsTo("tests");

    private final OptionSpec<String> cloudProvider = parser.accepts("cloudProvider",
            "Defines the cloud provider for your test setup."
                    + " Retrieve a list of valid cloud providers with --listCloudProviders."
    ).withRequiredArg().ofType(String.class).defaultsTo(PROVIDER_LOCAL);

    private final OptionSpec listCloudProvidersSpec = parser.accepts("listCloudProviders",
            "Prints a list of all supported cloud providers.");

    private final OptionSpec createSshCopyIdScriptSpec = parser.accepts("createSshCopyIdScript",
            format("Creates a script file with ssh-copy-id commands for all public IP addressed from the %s file.",
                    AgentsFile.NAME));

    private final OptionSpec sshConnectionCheckSpec = parser.accepts("sshConnectionCheck",
            format("Checks the SSH connection to all remote machines in the %s file.", AgentsFile.NAME));

    private final OptionSpec compareSimulatorPropertiesSpec = parser.accepts("compareSimulatorProperties",
            format("Compares the %s file in your working directory with the default property values.",
                    SimulatorProperties.PROPERTIES_FILE_NAME));

    private WizardCli() {
    }

    static Wizard init() {
        Wizard.logHeader();

        SimulatorProperties simulatorProperties = new SimulatorProperties();
        simulatorProperties.init(null);

        Bash bash = new Bash(simulatorProperties);

        return new Wizard(simulatorProperties, bash);
    }

    static void run(String[] args, Wizard wizard) {
        WizardCli cli = new WizardCli();
        OptionSet options = initOptionsWithHelp(cli.parser, args);

        if (options.has(cli.installSpec)) {
            String homeDir = System.getProperty("user.dir");
            wizard.install(getSimulatorPath(), getProfileFile(homeDir));
        } else if (options.has(cli.createWorkDirSpec)) {
            wizard.createWorkDir(cli.createWorkDirSpec.value(options), cli.cloudProvider.value(options));
        } else if (options.has(cli.listCloudProvidersSpec)) {
            wizard.listCloudProviders();
        } else if (options.has(cli.createSshCopyIdScriptSpec)) {
            wizard.createSshCopyIdScript();
        } else if (options.has(cli.sshConnectionCheckSpec)) {
            wizard.sshConnectionCheck();
        } else if (options.has(cli.compareSimulatorPropertiesSpec)) {
            wizard.compareSimulatorProperties();
        } else {
            printHelpAndExit(cli.parser);
        }
    }
}
