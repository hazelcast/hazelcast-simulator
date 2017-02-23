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
import org.apache.log4j.Logger;

import java.io.File;

import static com.hazelcast.simulator.common.GitInfo.getBuildTime;
import static com.hazelcast.simulator.common.GitInfo.getCommitIdAbbrev;
import static com.hazelcast.simulator.utils.CliUtils.initOptionsWithHelp;
import static com.hazelcast.simulator.utils.CliUtils.printHelpAndExit;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_LOCAL;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.FileUtils.getUserHomePath;
import static com.hazelcast.simulator.wizard.WizardUtils.getProfileFile;
import static com.hazelcast.simulator.wizard.WizardUtils.getSimulatorPath;
import static java.lang.String.format;

final class WizardCli {

    private static final Logger LOGGER = Logger.getLogger(WizardCli.class);

    // open for testing
    Wizard wizard;

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

    private final OptionSet options;

    WizardCli(String[] args) {
        options = initOptionsWithHelp(parser, args);
        wizard = new Wizard();
    }

    public void run() {
        if (options.has(installSpec)) {
            String homeDir = getUserHomePath();
            wizard.install(getSimulatorPath(), getProfileFile(homeDir));
        } else if (options.has(createWorkDirSpec)) {
            SimulatorProperties simulatorProperties = getSimulatorProperties(false);
            wizard.createWorkDir(simulatorProperties,
                    createWorkDirSpec.value(options),
                    cloudProvider.value(options));
        } else if (options.has(listCloudProvidersSpec)) {
            wizard.listCloudProviders();
        } else if (options.has(createSshCopyIdScriptSpec)) {
            wizard.createSshCopyIdScript(getSimulatorProperties());
        } else if (options.has(sshConnectionCheckSpec)) {
            SimulatorProperties simulatorProperties = getSimulatorProperties();
            wizard.sshConnectionCheck(simulatorProperties, newBash(simulatorProperties));
        } else if (options.has(compareSimulatorPropertiesSpec)) {
            wizard.compareSimulatorProperties();
        } else {
            printHelpAndExit(parser);
        }
    }

    private static SimulatorProperties getSimulatorProperties() {
        return getSimulatorProperties(true);
    }

    private static SimulatorProperties getSimulatorProperties(boolean initWithWorkingDirFile) {
        SimulatorProperties simulatorProperties = new SimulatorProperties();
        if (initWithWorkingDirFile) {
            simulatorProperties.init((File) null);
        }
        return simulatorProperties;
    }

    private static Bash newBash(SimulatorProperties simulatorProperties) {
        return new Bash(simulatorProperties);
    }

    public static void main(String[] args) {
        LOGGER.info("Hazelcast Simulator Wizard");
        LOGGER.info(format("Version: %s, Commit: %s, Build Time: %s",
                getSimulatorVersion(), getCommitIdAbbrev(), getBuildTime()));

        try {
            WizardCli cli = new WizardCli(args);
            cli.run();
        } catch (Exception e) {
            exitWithError(LOGGER, "Could not execute command", e);
        }
    }
}

