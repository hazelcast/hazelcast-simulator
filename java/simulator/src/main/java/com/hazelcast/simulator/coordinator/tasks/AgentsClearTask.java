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
import com.hazelcast.simulator.coordinator.registry.Registry;
import com.hazelcast.simulator.utils.BashCommand;
import org.apache.log4j.Logger;

import static com.hazelcast.simulator.utils.CommonUtils.getElapsedSeconds;
import static com.hazelcast.simulator.utils.FileUtils.newFile;
import static com.hazelcast.simulator.utils.NativeUtils.execute;
import static com.hazelcast.simulator.utils.FileUtils.locatePythonFile;
import static java.lang.String.format;

public class AgentsClearTask {
    private static final Logger LOGGER = Logger.getLogger(AgentsClearTask.class);

    private final Registry registry;

    public AgentsClearTask(Registry registry) {
        this.registry = registry;
    }

    public void run() {
        LOGGER.info(format("Cleaning Worker homes of %s machines...", registry.agentCount()));
        long started = System.nanoTime();

        new BashCommand(locatePythonFile("agents_clear.py"))
                .addParams(AgentData.toYaml(registry))
                .execute();

        long elapsed = getElapsedSeconds(started);
        LOGGER.info(format("Finished cleaning Worker homes of %s machines (%s seconds)",
                registry.agentCount(), elapsed));

    }
}
