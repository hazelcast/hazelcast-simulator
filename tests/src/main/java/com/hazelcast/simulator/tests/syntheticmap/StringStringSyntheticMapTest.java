package com.hazelcast.simulator.tests.syntheticmap;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

import java.util.Random;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.waitClusterSize;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKeys;
import static com.hazelcast.simulator.utils.GeneratorUtils.generateStrings;

public class StringStringSyntheticMapTest {

    private static final ILogger LOGGER = Logger.getLogger(StringStringSyntheticMapTest.class);

    private enum Operation {
        PUT,
        GET
    }

    // properties
    public String basename = StringStringSyntheticMapTest.class.getSimpleName();
    public int keyLength = 10;
    public int valueLength = 10;
    public int keyCount = 10000;
    public int valueCount = 10000;
    public KeyLocality keyLocality = KeyLocality.RANDOM;
    public int minNumberOfMembers = 0;

    public double putProb = 0.1;

    // probes
    public Probe putProbe;
    public Probe getProbe;

    private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();

    private HazelcastInstance hazelcastInstance;
    private SyntheticMap<String, String> map;

    private String[] keys;
    private String[] values;

    @Setup
    public void setUp(TestContext testContext) throws Exception {
        hazelcastInstance = testContext.getTargetInstance();
        HazelcastInstance targetInstance = testContext.getTargetInstance();
        map = targetInstance.getDistributedObject(SyntheticMapService.SERVICE_NAME, "map-" + testContext.getTestId());

        operationSelectorBuilder
                .addOperation(Operation.PUT, putProb)
                .addDefaultOperation(Operation.GET);
    }

    @Teardown
    public void tearDown() throws Exception {
        map.destroy();
        LOGGER.info(getOperationCountInformation(hazelcastInstance));
    }

    @Warmup(global = false)
    public void warmup() throws InterruptedException {
        waitClusterSize(LOGGER, hazelcastInstance, minNumberOfMembers);
        keys = generateStringKeys(keyCount, keyLength, keyLocality, hazelcastInstance);
        values = generateStrings(valueCount, valueLength);

        Random random = new Random();
        for (String key : keys) {
            String value = values[random.nextInt(valueCount)];
            map.put(key, value);
        }
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
        protected void timeStep(Operation operation) throws Exception {
            String key = randomKey();

            switch (operation) {
                case PUT:
                    String value = randomValue();
                    putProbe.started();
                    map.put(key, value);
                    putProbe.done();
                    break;
                case GET:
                    getProbe.started();
                    map.get(key);
                    getProbe.done();
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }

        private String randomKey() {
            return keys[randomInt(keys.length)];
        }

        private String randomValue() {
            return values[randomInt(values.length)];
        }
    }

    public static void main(String[] args) throws Exception {
        StringStringSyntheticMapTest test = new StringStringSyntheticMapTest();
        new TestRunner<StringStringSyntheticMapTest>(test).run();
    }
}
