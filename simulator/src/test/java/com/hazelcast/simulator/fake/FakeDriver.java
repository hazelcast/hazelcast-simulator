package com.hazelcast.simulator.fake;

import com.hazelcast.simulator.agent.workerprocess.WorkerParameters;
import com.hazelcast.simulator.vendors.VendorDriver;

import java.io.IOException;

public class FakeDriver extends VendorDriver {

    public FakeInstance instance = new FakeInstance();

    @Override
    public Object getVendorInstance() {
        return instance;
    }

    @Override
    public void startVendorInstance() throws Exception {
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
