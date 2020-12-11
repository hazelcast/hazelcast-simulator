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
import com.hazelcast.simulator.coordinator.registry.Registry;
import com.hazelcast.simulator.utils.BashCommand;
import com.hazelcast.simulator.utils.NativeUtils;

import static com.hazelcast.simulator.coordinator.registry.AgentData.publicAddressesString;
import static com.hazelcast.simulator.utils.FileUtils.getConfigurationFile;

public final class AgentUtils {

    private AgentUtils() {
    }

    public static void onlineCheckAgents(SimulatorProperties properties, Registry registry) {
        new BashCommand(getConfigurationFile("agent_online_check.sh").getAbsolutePath())
                .addParams(publicAddressesString(registry))
                .addEnvironment(properties.asMap())
                .dumpOutputOnError(true)
                .execute();
    }

    public static void startAgents(SimulatorProperties properties, Registry registry) {
        String startScript = getConfigurationFile("agent_start.sh").getAbsolutePath();

        new BashCommand(startScript)
                .ensureJavaOnPath()
                .addParams(publicAddressesString(registry))
                .addEnvironment(properties.asMap())
                .addEnvironment("parentPid", NativeUtils.getPID())
                .execute();
    }

    public static void stopAgents(SimulatorProperties properties, Registry registry) {
        String shutdownScript = getConfigurationFile("agent_shutdown.sh").getAbsolutePath();

        new BashCommand(shutdownScript)
                .addParams(publicAddressesString(registry))
                .addEnvironment(properties.asMap())
                .execute();
    }
}
