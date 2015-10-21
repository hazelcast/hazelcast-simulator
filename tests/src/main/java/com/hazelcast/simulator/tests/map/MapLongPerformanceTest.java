package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

public class MapLongPerformanceTest {

    private enum Operation {
        PUT,
        GET
    }

    // properties
    public String basename = MapLongPerformanceTest.class.getSimpleName();
    public int threadCount = 10;
    public int keyCount = 1000000;
    public double writeProb = 0.1;

    // probes
    public Probe probe;

    private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();

    private IMap<Integer, Long> map;

    @Setup
    public void setUp(TestContext testContext) {
        HazelcastInstance hazelcastInstance = testContext.getTargetInstance();
        map = hazelcastInstance.getMap(basename + '-' + testContext.getTestId());

        operationSelectorBuilder
                .addOperation(Operation.PUT, writeProb)
                .addDefaultOperation(Operation.GET);
    }

    @Teardown
    public void tearDown() throws Exception {
        map.destroy();
    }

    @Warmup(global = true)
    public void warmup() throws Exception {
        Streamer<Integer, Long> streamer = StreamerFactory.getInstance(map);
        for (int i = 0; i < keyCount; i++) {
            streamer.pushEntry(i, 0L);
        }
        streamer.await();
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorker<Operation> {

        public Worker() {
            super(operationSelectorBuilder);
        }

        @Override
        public void timeStep(Operation operation) {
            Integer key = randomInt(keyCount);

            switch (operation) {
                case PUT:
                    probe.started();
                    try {
                        map.set(key, System.currentTimeMillis());
                    } finally {
                        probe.done();
                    }
                    break;
                case GET:
                    probe.started();
                    try {
                        map.get(key);
                    } finally {
                        probe.done();
                    }
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        MapLongPerformanceTest test = new MapLongPerformanceTest();
        new TestRunner<MapLongPerformanceTest>(test).run();
    }
}
