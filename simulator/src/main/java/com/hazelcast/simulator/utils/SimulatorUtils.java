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
package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.common.AgentsFile;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.coordinator.registry.ComponentRegistry;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.File;

import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.newFile;

public final class SimulatorUtils {

    private SimulatorUtils() {
    }

    public static ComponentRegistry loadComponentRegister(File agentsFile) {
        return loadComponentRegister(agentsFile, false);
    }

    public static ComponentRegistry loadComponentRegister(File agentsFile, boolean sizeCheck) {
        ensureExistingFile(agentsFile);
        ComponentRegistry componentRegistry = AgentsFile.load(agentsFile);
        if (sizeCheck && componentRegistry.agentCount() == 0) {
            throw new CommandLineExitException("Agents file " + agentsFile + " is empty.");
        }
        return componentRegistry;
    }

    public static SimulatorProperties loadSimulatorProperties() {
        SimulatorProperties simulatorProperties = new SimulatorProperties();
        File file = new File(FileUtils.getUserDir(), "simulator.properties");
        if (file.exists()) {
            simulatorProperties.init(file);
        }

        return simulatorProperties;
    }

    static File getPropertiesFile(OptionSet options, OptionSpec<String> propertiesFileSpec) {
        if (options.has(propertiesFileSpec)) {
            // a file was explicitly configured
            return newFile(options.valueOf(propertiesFileSpec));
        } else {
            return null;
        }
    }
}
