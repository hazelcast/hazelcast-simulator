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
import com.hazelcast.simulator.coordinator.registry.AgentData;
import com.hazelcast.simulator.coordinator.registry.Registry;
import com.hazelcast.simulator.utils.Bash;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.Arrays;
import java.util.Properties;
import java.util.TreeSet;

import static com.hazelcast.simulator.common.AgentsFile.preferredAgentsFile;
import static com.hazelcast.simulator.common.SimulatorProperties.CLOUD_PROVIDER;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_EC2;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_LOCAL;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_STATIC;
import static com.hazelcast.simulator.utils.CloudProviderUtils.SUPPORTED_CLOUD_PROVIDERS;
import static com.hazelcast.simulator.utils.CloudProviderUtils.isEC2;
import static com.hazelcast.simulator.utils.CloudProviderUtils.isLocal;
import static com.hazelcast.simulator.utils.CloudProviderUtils.isStatic;
import static com.hazelcast.simulator.utils.CloudProviderUtils.isSupported;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.FormatUtils.HORIZONTAL_RULER;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static com.hazelcast.simulator.utils.NativeUtils.execute;
import static com.hazelcast.simulator.utils.SimulatorUtils.loadComponentRegister;
import static com.hazelcast.simulator.wizard.WizardUtils.containsCommentedOutProperty;
import static com.hazelcast.simulator.wizard.WizardUtils.copyResourceFile;
import static com.hazelcast.simulator.wizard.WizardUtils.getCommentedOutProperty;
import static java.lang.String.format;

class Wizard {

    static final File SSH_COPY_ID_FILE = new File("ssh-copy-id-script").getAbsoluteFile();

    private static final Logger LOGGER = Logger.getLogger(Wizard.class);

    private final File agentFile =  preferredAgentsFile().getAbsoluteFile();

    Wizard() {
    }

    void install(String simulatorPath, File profileFile) {
        echoImportant("Hazelcast Simulator Installation");

        echo("Found Simulator in the following directory: %s", simulatorPath);

        echo("Found the following profile file: %s", profileFile.getAbsolutePath());

        String profile = fileAsText(profileFile);
        if (profile.contains("SIMULATOR_HOME")) {
            throw new CommandLineExitException("Hazelcast Simulator seems to be already installed on this system!");
        }

        String config = NEW_LINE + "# Hazelcast Simulator configuration" + NEW_LINE
                + "export SIMULATOR_HOME=" + simulatorPath + NEW_LINE
                + "PATH=$SIMULATOR_HOME/bin:$PATH" + NEW_LINE;
        echo("Will append the following configuration to your profile file:%n%s", config);

        appendText(config, profileFile);
        echo("Done!%n%nNOTE: Don't forget to start a new terminal to make changes effective!");
    }

    void createWorkDir(SimulatorProperties simulatorProperties, String pathName, String cloudProvider) {
        File workDir = new File(pathName).getAbsoluteFile();
        if (workDir.exists()) {
            throw new CommandLineExitException(format("Working directory '%s' already exists!", workDir));
        }

        if (!isSupported(cloudProvider)) {
            throw new CommandLineExitException(
                    format("Unsupported cloud provider %s. Must be one of: %s",
                            workDir,
                            Arrays.asList(SUPPORTED_CLOUD_PROVIDERS))
            );
        }

        echo("Will create working directory '%s' for cloud provider '%s'", workDir, cloudProvider);
        ensureExistingDirectory(workDir);

        copyResourceFile(workDir, "runScript", "run");
        copyResourceFile(workDir, "testSuite", "test.properties");
        copyResourceFile(workDir, "example-member-config.xml", "hazelcast.xml");
        copyResourceFile(workDir, "example-client-config.xml", "client-hazelcast.xml");

        if (isLocal(cloudProvider)) {
            return;
        }

        ensureExistingFile(workDir, AgentsFile.NAME);

        File simulatorPropertiesFile = ensureExistingFile(workDir, SimulatorProperties.PROPERTIES_FILE_NAME);
        writeText(format("%s=%s%n", CLOUD_PROVIDER, cloudProvider), simulatorPropertiesFile);
        if (isEC2(cloudProvider)) {
            copyResourceFile(workDir, "awsEc2SimulatorProperties", "simulator.properties");
        }

        if (isStatic(cloudProvider)) {
            copyResourceFile(workDir, "prepareScriptStatic", "prepare");
        } else {
            copyResourceFile(workDir, "prepareScriptCloud", "prepare");
        }
    }

    void listCloudProviders() {
        echo("Supported cloud providers:");
        echo(" • %s: Local Setup", PROVIDER_LOCAL);
        echo(" • %s: Static Setup", PROVIDER_STATIC);
        echo(" • %s: EC2 Setup", PROVIDER_EC2);
    }

    void createSshCopyIdScript(SimulatorProperties simulatorProperties) {
        Registry registry = loadComponentRegister(agentFile, true);
        String userName = simulatorProperties.getUser();

        ensureExistingFile(SSH_COPY_ID_FILE);
        writeText("#!/bin/bash" + NEW_LINE + NEW_LINE, SSH_COPY_ID_FILE);
        for (AgentData agent : registry.getAgents()) {
            String publicAddress = agent.getPublicAddress();
            appendText(format("ssh-copy-id -i ~/.ssh/id_rsa.pub %s@%s%n", userName, publicAddress), SSH_COPY_ID_FILE);
        }
        execute(format("chmod u+x %s", SSH_COPY_ID_FILE.getAbsoluteFile()));

        echo("Please execute './%s' to copy your public RSA key to all remote machines.", SSH_COPY_ID_FILE.getName());
    }

    void sshConnectionCheck(SimulatorProperties simulatorProperties, Bash bash) {
        if (isLocal(simulatorProperties)) {
            throw new CommandLineExitException("SSH is not supported for local setups.");
        }

        Registry registry = loadComponentRegister(agentFile, true);
        String userName = simulatorProperties.getUser();

        for (AgentData agent : registry.getAgents()) {
            String publicAddress = agent.getPublicAddress();
            echo("Connecting to %s@%s...", userName, publicAddress);
            bash.ssh(publicAddress, "echo ok 2>&1");
        }

        echo("Connected successfully to all remote machines!");
    }

    void compareSimulatorProperties() {
        SimulatorProperties defaultProperties = new SimulatorProperties();
        String defaultPropertiesString = defaultProperties.getDefaultsAsString();
        Properties userProperties = WizardUtils.getUserProperties();

        int size = userProperties.size();
        if (size == 0) {
            echo(format("Found no %s file or file was empty!", SimulatorProperties.PROPERTIES_FILE_NAME));
            return;
        }

        echo("Defined user properties:");
        int unknownProperties = 0;
        int changedProperties = 0;
        for (String property : new TreeSet<>(userProperties.stringPropertyNames())) {
            boolean commentedOutProperty = containsCommentedOutProperty(defaultPropertiesString, property);
            String userValue = userProperties.getProperty(property);
            String defaultValue = (commentedOutProperty ? getCommentedOutProperty(defaultPropertiesString, property)
                    : defaultProperties.get(property));

            if (!defaultProperties.containsKey(property) && !commentedOutProperty) {
                echo("%s = %s [unknown property]", property, userValue);
                unknownProperties++;
            } else if (!userValue.equals(defaultValue)) {
                echo("%s = %s [default: %s]", property, userValue, defaultValue);
                changedProperties++;
            } else {
                echo("%s = %s", property, userValue);
            }
        }

        logResults(size, unknownProperties, changedProperties);
    }

    private void logResults(int size, int unknownProperties, int changedProperties) {
        StringBuilder sb = new StringBuilder();
        sb.append(size).append((size > 1 ? " properties" : " property")).append(" defined");
        if (changedProperties > 0) {
            sb.append(" (").append(changedProperties).append(" changed)");
        }
        if (unknownProperties > 0) {
            sb.append(" (").append(unknownProperties).append(" unknown)");
        }
        echo(sb.toString());
        if (unknownProperties > 0) {
            LOGGER.warn("Unknown properties will be ignored!");
        }
    }


    private static void echo(String message, Object... args) {
        LOGGER.info(message == null ? "null" : format(message, args));
    }

    private static void echoImportant(String message, Object... args) {
        echo(HORIZONTAL_RULER);
        echo(message, args);
        echo(HORIZONTAL_RULER);
    }
}
