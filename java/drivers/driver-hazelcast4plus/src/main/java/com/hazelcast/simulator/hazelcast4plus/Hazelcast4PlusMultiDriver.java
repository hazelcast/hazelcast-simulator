package com.hazelcast.simulator.hazelcast4plus;

import com.hazelcast.simulator.drivers.Driver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Hazelcast4PlusMultiDriver
        extends Driver<HazelcastInstances> {

    private static final String CLIENT_COUNT_PROPERTY = "clients_per_loadgenerator";
    private static final String WORKER_TYPE_PROPERTY = "WORKER_TYPE";
    private static final String CLIENT_TYPE = "javaclient";

    private final List<Hazelcast4PlusDriver> delegates = new ArrayList<>();
    private HazelcastInstances instances;

    @Override
    public HazelcastInstances getDriverInstance() {
        return instances;
    }

    @Override
    public void startDriverInstance()
            throws Exception {
        String workerType = get(WORKER_TYPE_PROPERTY);
        int clientsPerLoadGenerator = Integer.parseInt(get(CLIENT_COUNT_PROPERTY, "1"));
        int instancesCount = CLIENT_TYPE.equals(workerType) ? clientsPerLoadGenerator : 1;
        for (int i = 0; i < instancesCount; i++) {
            Hazelcast4PlusDriver delegate = new Hazelcast4PlusDriver();
            delegate.setAll(properties);
            delegate.startDriverInstance();
            delegates.add(delegate);
        }
        instances = new HazelcastInstances(delegates.stream().map(Hazelcast4PlusDriver::getDriverInstance).toList());
    }

    @Override
    public void close()
            throws IOException {
        IOException err = null;
        for (Hazelcast4PlusDriver delegate : delegates) {
            try {
                delegate.close();
            } catch (IOException e) {
                if (err == null) {
                    err = e;
                } else {
                    err.addSuppressed(e);
                }
            }
        }
        if (err != null) throw err;
    }
}
