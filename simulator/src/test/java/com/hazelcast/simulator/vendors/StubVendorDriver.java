package com.hazelcast.simulator.vendors;

import com.hazelcast.simulator.agent.workerprocess.WorkerParameters;
import com.hazelcast.simulator.coordinator.registry.AgentData;

import java.io.IOException;

public class StubVendorDriver extends VendorDriver {
    @Override
    public Object getVendorInstance() {
        return null;
    }

    @Override
    public void createVendorInstance() throws Exception {

    }

    @Override
    public WorkerParameters loadWorkerParameters(String workerType, int agentIndex) {
        return new WorkerParameters()
                .setAll(properties)
                .set("WORKER_TYPE", workerType);
    }

    @Override
    public void close() throws IOException {

    }
}
