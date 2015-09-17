package com.hazelcast.simulator.protocol.registry;

import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;

/**
 * Contains the metadata of a Simulator Worker.
 *
 * Part of the metadata is the {@link SimulatorAddress} which is used by the Simulator Communication Protocol.
 */
public class WorkerData {

    private final SimulatorAddress address;
    private final WorkerJvmSettings settings;

    public WorkerData(SimulatorAddress parentAddress, WorkerJvmSettings settings) {
        this.address = parentAddress.getChild(settings.getWorkerIndex());
        this.settings = settings;
    }

    public SimulatorAddress getAddress() {
        return address;
    }

    public WorkerJvmSettings getSettings() {
        return settings;
    }
}
