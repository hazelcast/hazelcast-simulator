package com.hazelcast.stabilizer.tests.syntheticmap;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.probes.probes.IntervalProbe;
import com.hazelcast.stabilizer.probes.probes.SimpleProbe;
import com.hazelcast.stabilizer.test.TestContext;
import com.hazelcast.stabilizer.test.TestRunner;
import com.hazelcast.stabilizer.test.annotations.RunWithWorker;
import com.hazelcast.stabilizer.test.annotations.Setup;
import com.hazelcast.stabilizer.test.annotations.Teardown;
import com.hazelcast.stabilizer.test.annotations.Warmup;
import com.hazelcast.stabilizer.tests.helpers.KeyLocality;
import com.hazelcast.stabilizer.tests.helpers.KeyUtils;
import com.hazelcast.stabilizer.tests.helpers.StringUtils;
import com.hazelcast.stabilizer.worker.selector.OperationSelectorBuilder;
import com.hazelcast.stabilizer.worker.tasks.AbstractWorkerTask;

import java.util.Random;

import static com.hazelcast.stabilizer.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.stabilizer.tests.helpers.HazelcastTestUtils.waitClusterSize;

public class StringStringSyntheticMapTest {

    private static final ILogger log = Logger.getLogger(StringStringSyntheticMapTest.class);

    private enum Operation {
        PUT,
        GET
    }

    // properties
    public int keyLength = 10;
    public int valueLength = 10;
    public int keyCount = 10000;
    public int valueCount = 10000;
    public String basename = "stringStringMap";
    public KeyLocality keyLocality = KeyLocality.Random;
    public int minNumberOfMembers = 0;

    public double putProb = 0.1;

    // probes
    public IntervalProbe putLatency;
    public IntervalProbe getLatency;
    public SimpleProbe throughput;

    private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();

    private TestContext testContext;
    private SyntheticMap<String, String> map;

    private String[] keys;
    private String[] values;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        HazelcastInstance targetInstance = testContext.getTargetInstance();
        map = targetInstance.getDistributedObject(SyntheticMapService.SERVICE_NAME, "map-" + testContext.getTestId());
        operationSelectorBuilder.addOperation(Operation.PUT, putProb).addDefaultOperation(Operation.GET);
    }

    @Teardown
    public void teardown() throws Exception {
        map.destroy();
        log.info(getOperationCountInformation(testContext.getTargetInstance()));
    }

    @Warmup(global = false)
    public void warmup() throws InterruptedException {
        waitClusterSize(log, testContext.getTargetInstance(), minNumberOfMembers);
        keys = KeyUtils.generateStringKeys(keyCount, keyLength, keyLocality, testContext.getTargetInstance());
        values = StringUtils.generateStrings(valueCount, valueLength);

        Random random = new Random();
        for (String key : keys) {
            String value = values[random.nextInt(valueCount)];
            map.put(key, value);
        }
    }

    @RunWithWorker
    public AbstractWorkerTask<Operation> createBaseWorker() {
        return new WorkerTask();
    }

    private class WorkerTask extends AbstractWorkerTask<Operation> {

        public WorkerTask() {
            super(operationSelectorBuilder);
        }

        @Override
        protected void doRun(Operation operation) {
            String key = randomKey();

            switch (operation) {
                case PUT:
                    String value = randomValue();
                    putLatency.started();
                    map.put(key, value);
                    putLatency.done();
                    break;
                case GET:
                    getLatency.started();
                    map.get(key);
                    getLatency.done();
                    break;
                default:
                    throw new UnsupportedOperationException();
            }

            throughput.done();
        }

        private String randomKey() {
            return keys[nextInt(keys.length)];
        }

        private String randomValue() {
            return values[nextInt(values.length)];
        }
    }

    public static void main(String[] args) throws Throwable {
        StringStringSyntheticMapTest test = new StringStringSyntheticMapTest();
        new TestRunner<StringStringSyntheticMapTest>(test).run();
    }
}
