package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.common.AgentsFile;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;

import java.io.File;

import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;

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
}
