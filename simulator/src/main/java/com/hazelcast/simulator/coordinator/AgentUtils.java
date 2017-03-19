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
package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.coordinator.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.BashCommand;

import static com.hazelcast.simulator.utils.FileUtils.getConfigurationFile;
import static com.hazelcast.simulator.utils.FormatUtils.join;

public final class AgentUtils {

    private AgentUtils() {
    }

    public static void sslTestAgents(SimulatorProperties properties, ComponentRegistry registry) {
        new BashCommand(getConfigurationFile("agent_ssh_check.sh").getAbsolutePath())
                .addParams(join(registry.getAgentIps(), ","))
                .addEnvironment(properties.asMap())
                .dumpOutputOnError(false)
                .execute();
    }

    public static void startAgents(SimulatorProperties properties, ComponentRegistry registry) {
        String startScript = getConfigurationFile("agent_start.sh").getAbsolutePath();

        new BashCommand(startScript)
                .addParams(join(registry.getAgentIps(), ","))
                .addEnvironment(properties.asMap())
                .execute();
    }

    public static void stopAgents(SimulatorProperties properties, ComponentRegistry registry) {
        String shutdownScript = getConfigurationFile("agent_shutdown.sh").getAbsolutePath();

        new BashCommand(shutdownScript)
                .addParams(join(registry.getAgentIps(), ","))
                .addEnvironment(properties.asMap())
                .execute();
    }
}
