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
import com.hazelcast.simulator.utils.CloudProviderUtils;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.apache.log4j.Logger;
import org.jclouds.providers.ProviderMetadata;
import org.jclouds.providers.Providers;

import java.io.File;

import static com.hazelcast.simulator.common.GitInfo.getBuildTime;
import static com.hazelcast.simulator.common.GitInfo.getCommitIdAbbrev;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.getResourceFile;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.FormatUtils.HORIZONTAL_RULER;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static com.hazelcast.simulator.utils.NativeUtils.execute;
import static com.hazelcast.simulator.wizard.WizardCli.init;
import static com.hazelcast.simulator.wizard.WizardCli.run;
import static java.lang.String.format;

public class Wizard {

    private static final Logger LOGGER = Logger.getLogger(Wizard.class);

    Wizard() {
        echo("Hazelcast Simulator Wizard");
        echo("Version: %s, Commit: %s, Build Time: %s", getSimulatorVersion(), getCommitIdAbbrev(), getBuildTime());
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

        File runScript = ensureExistingFile(workDir, "run");
        writeText(getResourceFile("runScript"), runScript);
        execute(format("chmod u+x %s", runScript.getAbsolutePath()));

        File testProperties = ensureExistingFile(workDir, "test.properties");
        writeText("IntIntMapTest@class = com.hazelcast.simulator.tests.map.IntIntMapTest" + NEW_LINE, testProperties);

        if (!CloudProviderUtils.isLocal(cloudProvider)) {
            File simulatorProperties = ensureExistingFile(workDir, SimulatorProperties.PROPERTIES_FILE_NAME);
            writeText(format("CLOUD_PROVIDER=%s%n", cloudProvider), simulatorProperties);

            ensureExistingFile(workDir, AgentsFile.NAME);
        }
    }

    void listCloudProviders() {
        echo("Supported cloud providers:");
        echo(" • %s: Local Setup", CloudProviderUtils.PROVIDER_LOCAL);
        echo(" • %s: Static Setup", CloudProviderUtils.PROVIDER_STATIC);
        for (ProviderMetadata providerMetadata : Providers.all()) {
            echo(" • %s: %s", providerMetadata.getId(), providerMetadata.getName());
        }
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
