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

import com.hazelcast.simulator.coordinator.registry.AgentData;
import com.hazelcast.simulator.utils.BashCommand;

import java.io.File;
import java.util.List;
import java.util.Map;

import static com.hazelcast.simulator.utils.FileUtils.getConfigurationFile;
import static com.hazelcast.simulator.utils.FileUtils.locatePythonFile;
import static com.hazelcast.simulator.utils.FormatUtils.join;

/**
 * Prepares the run on the remote machines.
 *
 * This includes creating the run directory, uploading the 'uploads' directory.
 *
 * The real work is done by the 'prepare_run.py' script.
 */
public class PrepareRunTask {
    private final List<AgentData> agents;
    private final Map<String, String> simulatorProperties;
    private final File uploadDir;
    private final String runId;

    public PrepareRunTask(List<AgentData> agents,
                          Map<String, String> simulatorProperties,
                          File uploadDir,
                          String runId) {
        this.agents = agents;
        this.simulatorProperties = simulatorProperties;
        this.uploadDir = uploadDir;
        this.runId = runId;
    }

    public void run() {
        String installFile = locatePythonFile("prepare_run.py");
        new BashCommand(installFile)
                .addEnvironment(simulatorProperties)
                .addParams(uploadDir.getAbsolutePath(), runId, AgentData.toYaml(agents))
                .execute();

    }
}
