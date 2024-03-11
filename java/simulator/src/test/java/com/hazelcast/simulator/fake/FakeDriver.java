package com.hazelcast.simulator.fake;

import com.hazelcast.simulator.agent.workerprocess.WorkerParameters;
import com.hazelcast.simulator.drivers.Driver;

import java.io.IOException;

public class FakeDriver extends Driver<FakeInstance> {

    public FakeInstance instance = new FakeInstance();

    @Override
    public FakeInstance getDriverInstance() {
        return instance;
    }

    @Override
    public void startDriverInstance() throws Exception {
    }

    @Override
    public void close() throws IOException {
    }
}
