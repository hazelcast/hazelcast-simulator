package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.utils.GeneratorUtils;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.waitClusterSize;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKey;

public class MapPutAllTest {

    private static final ILogger LOGGER = Logger.getLogger(MapPutAllTest.class);

    // properties
    public String basename = MapPutAllTest.class.getSimpleName();
    public int minNumberOfMembers = 0;
    // number of items in a single map to insert
    public int itemCount = 10000;
    // the number of characters in the key
    public int keySize = 10;
    // the number of characters in the value
    public int valueSize = 100;
    // controls the key locality. E.g. a batch can be made for local or single partition etc.
    public KeyLocality keyLocality;
    // the number of maps we insert. We don't want to keep inserting the same map over an over
    public int mapCount = 2;
    // if we want to use putAll or put (this is a nice setting to see what kind of speedup or slowdown to expect)
    public boolean usePutAll = true;

    private IMap<String, String> map;
    private HazelcastInstance instance;
    private Map<String, String>[] inputMaps;

    @Setup
    public void setUp(TestContext testContext) throws Exception {
        instance = testContext.getTargetInstance();
        map = testContext.getTargetInstance().getMap(basename + "-" + testContext.getTestId());
    }

    @Teardown
    public void tearDown() throws Exception {
        map.destroy();
        LOGGER.info(getOperationCountInformation(instance));
    }

    @Warmup(global = false)
    @SuppressWarnings("unchecked")
    public void warmup() throws InterruptedException {
        waitClusterSize(LOGGER, instance, minNumberOfMembers);

        String[] keys = new String[itemCount];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = generateStringKey(keySize, keyLocality, instance);
        }

        inputMaps = new Map[mapCount];
        for (int mapIndex = 0; mapIndex < mapCount; mapIndex++) {
            Map<String, String> inputMap = new HashMap<String, String>();
            inputMaps[mapIndex] = inputMap;
            for (int itemIndex = 0; itemIndex < itemCount; itemIndex++) {
                String value = GeneratorUtils.generateString(valueSize);
                inputMap.put(keys[mapIndex], value);
            }
        }
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractMonotonicWorker {

        @Override
        protected void timeStep() throws Exception {
            Map<String, String> insertMap = randomMap();
            if (usePutAll) {
                map.putAll(insertMap);
            } else {
                for (Map.Entry<String, String> entry : insertMap.entrySet()) {
                    map.put(entry.getKey(), entry.getValue());
                }
            }
        }

        private Map<String, String> randomMap() {
            return inputMaps[randomInt(inputMaps.length)];
        }
    }

    public static void main(String[] args) throws Exception {
        MapPutAllTest test = new MapPutAllTest();
        new TestRunner<MapPutAllTest>(test).withDuration(10).run();
    }
}
