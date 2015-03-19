package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.simulator.probes.probes.IntervalProbe;
import com.hazelcast.simulator.probes.probes.SimpleProbe;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.Name;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Warmup;
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
    private SimpleProbe setProbe;
    private IntervalProbe intervalProbe;
    private SimpleProbe getProbe;

    private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();

    private IMap<Integer, Long> map;

    @Setup
    public void setup(TestContext testContext, @Name("latencyProbe")IntervalProbe intervalProbe,
                      @Name("set") SimpleProbe setProbe, @Name("get") SimpleProbe getProbe) {
        this.intervalProbe = intervalProbe;
        this.setProbe = setProbe;
        this.getProbe = getProbe;

        HazelcastInstance hazelcastInstance = testContext.getTargetInstance();
        map = hazelcastInstance.getMap(basename + "-" + testContext.getTestId());

        operationSelectorBuilder
                .addOperation(Operation.PUT, writeProb)
                .addDefaultOperation(Operation.GET);
    }

    @Teardown
    public void teardown() throws Exception {
        map.destroy();
    }

    @Warmup(global = true)
    public void warmup() throws Exception {
        for (int i = 0; i < keyCount; i++) {
            map.put(i, 0l);
        }
    }

    @RunWithWorker
    public AbstractWorker<Operation> createWorker() {
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
                    intervalProbe.started();
                    try {
                        map.set(key, System.currentTimeMillis());
                    } finally {
                        intervalProbe.done();
                    }
                    setProbe.done();
                    break;
                case GET:
                    intervalProbe.started();
                    try {
                        map.get(key);
                    } finally {
                        intervalProbe.done();
                    }
                    getProbe.done();
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }
    }

    public static void main(String[] args) throws Throwable {
        MapLongPerformanceTest test = new MapLongPerformanceTest();
        new TestRunner<MapLongPerformanceTest>(test).run();
    }
}
