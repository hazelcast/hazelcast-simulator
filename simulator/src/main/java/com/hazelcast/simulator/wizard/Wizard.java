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

import com.hazelcast.simulator.utils.CommandLineExitException;
import org.apache.log4j.Logger;

import java.io.File;

import static com.hazelcast.simulator.common.GitInfo.getBuildTime;
import static com.hazelcast.simulator.common.GitInfo.getCommitIdAbbrev;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FormatUtils.HORIZONTAL_RULER;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
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
