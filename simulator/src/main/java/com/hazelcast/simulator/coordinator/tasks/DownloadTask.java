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
package com.hazelcast.simulator.coordinator.tasks;

import com.hazelcast.simulator.utils.BashCommand;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.List;
import java.util.Map;

import static com.hazelcast.simulator.utils.FileUtils.getConfigurationFile;
import static com.hazelcast.simulator.utils.FormatUtils.join;

/**
 * Downloads and post-processes the artifacts from either local or remote machine
 *
 * The real work is done by the 'download.sh' script.
 */
public class DownloadTask {
    private static final Logger LOGGER = Logger.getLogger(DownloadTask.class);

    private final List<String> agents;
    private final Map<String, String> simulatorProperties;
    private final File rootDir;
    private final String sessionId;

    public DownloadTask(List<String> agents,
                        Map<String, String> simulatorProperties,
                        File rootDir,
                        String sessionId) {
        this.agents = agents;
        this.simulatorProperties = simulatorProperties;
        this.rootDir = rootDir;
        this.sessionId = sessionId;
    }

    public void run() {
        LOGGER.info("Downloading sessions [" + sessionId + "]");

        String installFile = getConfigurationFile("download.sh").getAbsolutePath();
        String agentIps = join(agents, ",");
        // setThrowsException(true), which causes to throw and exception rather than
        // calling System.exit(), is set on purpose. DownloadTask is run in the shutdown hook mechanism.
        // If an error occurs during DownloadTask in the shutdown process, without "true" it yields to deadlock.
        new BashCommand(installFile)
                .ensureJavaOnPath()
                .addEnvironment(simulatorProperties)
                .addParams(rootDir.getAbsolutePath(), sessionId, agentIps)
                .setThrowsException(true)
                .execute();

        LOGGER.info("Downloading complete!");
    }
}
