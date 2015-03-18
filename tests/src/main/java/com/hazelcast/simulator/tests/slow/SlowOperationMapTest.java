package com.hazelcast.simulator.tests.slow;

import com.hazelcast.core.IMap;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.spi.impl.InternalOperationService;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.tests.helpers.KeyUtils;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getNode;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.waitClusterSize;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.ReflectionUtils.getMethodByName;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokeMethod;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This test invokes slowed down map operations on a Hazelcast instance to provoke slow operation logs.
 */
public class SlowOperationMapTest {
    private static final ILogger log = Logger.getLogger(SlowOperationMapTest.class);

    private enum Operation {
        PUT,
        GET
    }

    // properties
    public int keyLength = 10;
    public int valueLength = 10;
    public int keyCount = 100;
    public int valueCount = 100;
    public String basename = "slowOperationTestMap";
    public KeyLocality keyLocality = KeyLocality.Random;
    public int minNumberOfMembers = 0;
    public double putProb = 0.5;
    public int recursionDepth = 10;

    private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();
    private final AtomicLong putCounter = new AtomicLong(0);
    private final AtomicLong getCounter = new AtomicLong(0);

    private TestContext testContext;
    private IMap<Integer, Integer> map;
    private Method getSlowOperationLogsMethod;
    private int[] keys;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        map = testContext.getTargetInstance().getMap(basename + "-" + testContext.getTestId());

        operationSelectorBuilder.addOperation(Operation.PUT, putProb)
                                .addDefaultOperation(Operation.GET);

        // try to find the getSlowOperationLogs method (since Hazelcast 3.5)
        getSlowOperationLogsMethod = getMethodByName(InternalOperationService.class, "getSlowOperationLogs");
        if (getSlowOperationLogsMethod == null) {
            fail("This test needs Hazelcast 3.5 or newer");
        }
    }

    @Teardown
    public void teardown() throws Exception {
        map.destroy();
        log.info(getOperationCountInformation(testContext.getTargetInstance()));
    }

    @Warmup(global = false)
    public void warmup() throws InterruptedException {
        waitClusterSize(log, testContext.getTargetInstance(), minNumberOfMembers);
        keys = KeyUtils.generateIntKeys(keyCount, Integer.MAX_VALUE, keyLocality, testContext.getTargetInstance());

        Random random = new Random();
        for (int key : keys) {
            int value = random.nextInt(Integer.MAX_VALUE);
            map.put(key, value);
        }

        // add the interceptor after the warmup, otherwise this stage will take ages
        map.addInterceptor(new SlowMapInterceptor(recursionDepth));
    }

    @Verify(global = true)
    public void verify() throws Exception {
        long operationCount = putCounter.get() + getCounter.get();
        assertTrue("Expected at least one completed operations, but was " + operationCount, operationCount > 0);

        Collection<Object> logs = null;
        try {
            Node node = getNode(testContext.getTargetInstance());
            InternalOperationService operationService = ((InternalOperationService) node.nodeEngine.getOperationService());
            logs = invokeMethod(operationService, getSlowOperationLogsMethod);
        } catch (Throwable t) {
            ExceptionReporter.report(testContext.getTestId(), t);
        }
        if (logs == null) {
            fail("Could not retrieve slow operation logs");
        }

        long actual = logs.size();
        long expected = Math.max(putCounter.get(), 1) + Math.max(getCounter.get(), 1);
        assertTrue("Expected " + expected + " slow operation logs, but was " + actual, actual == expected);

        log.info("Found " + actual + " slow query logs after completing " + operationCount + " operations.");
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
        protected void timeStep(Operation operation) {
            int key = randomKey();

            switch (operation) {
                case PUT:
                    map.put(key, randomValue());
                    putCounter.incrementAndGet();
                    break;
                case GET:
                    map.get(key);
                    getCounter.incrementAndGet();
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }

        private int randomKey() {
            return keys[randomInt(keys.length)];
        }

        private int randomValue() {
            return randomInt(Integer.MAX_VALUE);
        }
    }

    private static class SlowMapInterceptor implements MapInterceptor {
        private final int recursionDepth;

        public SlowMapInterceptor(int recursionDepth) {
            this.recursionDepth = recursionDepth;
        }

        @Override
        public Object interceptGet(Object value) {
            return null;
        }

        @Override
        public void afterGet(Object value) {
            sleepRecursion(recursionDepth, 15);
        }

        @Override
        public Object interceptPut(Object oldValue, Object newValue) {
            return null;
        }

        @Override
        public void afterPut(Object value) {
            sleepRecursion(recursionDepth, 20);
        }

        @Override
        public Object interceptRemove(Object removedValue) {
            return null;
        }

        @Override
        public void afterRemove(Object removedValue) {
        }

        private void sleepRecursion(int recursionDepth, int sleepSeconds) {
            if (recursionDepth == 0) {
                sleepSeconds(sleepSeconds);
                return;
            }
            sleepRecursion(recursionDepth - 1, sleepSeconds);
        }
    }

    public static void main(String[] args) throws Throwable {
        SlowOperationMapTest test = new SlowOperationMapTest();
        new TestRunner<SlowOperationMapTest>(test).run();
    }
}
