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
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.Bash;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.apache.log4j.Logger;
import org.jclouds.providers.ProviderMetadata;
import org.jclouds.providers.Providers;

import java.io.File;

import static com.hazelcast.simulator.common.GitInfo.getBuildTime;
import static com.hazelcast.simulator.common.GitInfo.getCommitIdAbbrev;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_LOCAL;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_STATIC;
import static com.hazelcast.simulator.utils.CloudProviderUtils.isLocal;
import static com.hazelcast.simulator.utils.CloudProviderUtils.isStatic;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.FormatUtils.HORIZONTAL_RULER;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static com.hazelcast.simulator.utils.NativeUtils.execute;
import static com.hazelcast.simulator.utils.SimulatorUtils.loadComponentRegister;
import static com.hazelcast.simulator.wizard.WizardCli.init;
import static com.hazelcast.simulator.wizard.WizardCli.run;
import static com.hazelcast.simulator.wizard.WizardUtils.createScriptFile;
import static java.lang.String.format;

public class Wizard {

    static final File SSH_COPY_ID_FILE = new File("ssh-copy-id-script").getAbsoluteFile();
    static final File AGENTS_FILE = new File(AgentsFile.NAME).getAbsoluteFile();

    private static final Logger LOGGER = Logger.getLogger(Wizard.class);

    private final SimulatorProperties simulatorProperties;
    private final Bash bash;

    Wizard(SimulatorProperties simulatorProperties, Bash bash) {
        echo("Hazelcast Simulator Wizard");
        echo("Version: %s, Commit: %s, Build Time: %s", getSimulatorVersion(), getCommitIdAbbrev(), getBuildTime());

        this.simulatorProperties = simulatorProperties;
        this.bash = bash;
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

    void createWorkDir(String pathName, String cloudProvider) {
        File workDir = new File(pathName).getAbsoluteFile();
        if (workDir.exists()) {
            throw new CommandLineExitException(format("Working directory '%s' already exists!", workDir));
        }

        echo("Will create working directory '%s' for cloud provider '%s'", workDir, cloudProvider);
        ensureExistingDirectory(workDir);

        createScriptFile(workDir, "run", "runScript");

        File testProperties = ensureExistingFile(workDir, "test.properties");
        writeText("IntIntMapTest@class = com.hazelcast.simulator.tests.map.IntIntMapTest" + NEW_LINE, testProperties);

        if (!isLocal(cloudProvider)) {
            File simulatorPropertiesFile = ensureExistingFile(workDir, SimulatorProperties.PROPERTIES_FILE_NAME);
            writeText(format("CLOUD_PROVIDER=%s%n", cloudProvider), simulatorPropertiesFile);

            if (isStatic(cloudProvider)) {
                createScriptFile(workDir, "prepare", "staticPrepareScript");
            } else {
                createScriptFile(workDir, "prepare", "cloudPrepareScript");
            }

            ensureExistingFile(workDir, AgentsFile.NAME);
        }
    }

    void listCloudProviders() {
        echo("Supported cloud providers:");
        echo(" • %s: Local Setup", PROVIDER_LOCAL);
        echo(" • %s: Static Setup", PROVIDER_STATIC);
        for (ProviderMetadata providerMetadata : Providers.all()) {
            echo(" • %s: %s", providerMetadata.getId(), providerMetadata.getName());
        }
    }

    void createSshCopyIdScript() {
        ComponentRegistry componentRegistry = loadComponentRegister(AGENTS_FILE, true);
        String userName = simulatorProperties.getUser();

        ensureExistingFile(SSH_COPY_ID_FILE);
        writeText("#!/bin/bash" + NEW_LINE + NEW_LINE, SSH_COPY_ID_FILE);
        for (AgentData agentData : componentRegistry.getAgents()) {
            String publicAddress = agentData.getPublicAddress();
            appendText(format("ssh-copy-id -i ~/.ssh/id_rsa.pub %s@%s%n", userName, publicAddress), SSH_COPY_ID_FILE);
        }
        execute(format("chmod u+x %s", SSH_COPY_ID_FILE.getAbsoluteFile()));

        echo("Please execute './%s' to copy your public RSA key to all remote machines.", SSH_COPY_ID_FILE.getName());
    }

    void sshConnectionCheck() {
        if (isLocal(simulatorProperties)) {
            throw new CommandLineExitException("SSH is not supported for local setups.");
        }

        ComponentRegistry componentRegistry = loadComponentRegister(AGENTS_FILE, true);
        String userName = simulatorProperties.getUser();

        for (AgentData agentData : componentRegistry.getAgents()) {
            String publicAddress = agentData.getPublicAddress();
            echo("Connecting to %s@%s...", userName, publicAddress);
            bash.ssh(publicAddress, "echo ok 2>&1");
        }

        echo("Connected successfully to all remote machines!");
    }

    private void echo(String message, Object... args) {
        LOGGER.info(message == null ? "null" : format(message, args));
    }

    private void echoImportant(String message, Object... args) {
        echo(HORIZONTAL_RULER);
        echo(message, args);
        echo(HORIZONTAL_RULER);
    }

    public static void main(String[] args) {
        try {
            run(args, init());
        } catch (Exception e) {
            exitWithError(LOGGER, "Could not execute command", e);
        }
    }
}
