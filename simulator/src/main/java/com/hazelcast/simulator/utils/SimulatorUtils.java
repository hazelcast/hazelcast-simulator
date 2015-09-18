package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.common.AgentsFile;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
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

    public static SimulatorProperties loadSimulatorProperties(OptionSet options, OptionSpec<String> propertiesFileSpec) {
        SimulatorProperties simulatorProperties = new SimulatorProperties();
        simulatorProperties.init(getPropertiesFile(options, propertiesFileSpec));

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
