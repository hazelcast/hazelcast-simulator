package com.hazelcast.simulator.tests.map;

import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.map.helpers.MapMaxSizeOperationCounter;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.worker.selector.OperationSelector;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.PER_NODE;
import static com.hazelcast.simulator.test.utils.TestUtils.assertEqualsStringFormat;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isMemberNode;
import static java.lang.String.format;
import static junit.framework.Assert.assertTrue;

/**
 * This tests runs {@link IMap#put(Object, Object)} and {@link IMap#get(Object)} operations on a map, which is configured with
 * {@link MaxSizeConfig.MaxSizePolicy#PER_NODE}.
 *
 * With some probability distribution we are doing put, putAsync, get and verification operations on the map.
 * We verify during the test and at the end that the map has not exceeded its max configured size.
 */
public class MapMaxSizeTest {

    private enum MapOperation {
        PUT,
        GET,
        CHECK_SIZE
    }

    private enum MapPutOperation {
        PUT_SYNC,
        PUT_ASYNC
    }

    private static final ILogger log = Logger.getLogger(MapMaxSizeTest.class);

    // properties
    public String basename = "MapMaxSize1";
    public int keyCount = Integer.MAX_VALUE;

    public double putProb = 0.5;
    public double getProb = 0.4;
    public double checkProb = 0.1;

    public double putUsingAsyncProb = 0.2;

    private final OperationSelectorBuilder<MapOperation> mapOperationSelectorBuilder
            = new OperationSelectorBuilder<MapOperation>();
    private final OperationSelectorBuilder<MapPutOperation> mapPutOperationSelectorBuilder
            = new OperationSelectorBuilder<MapPutOperation>();

    private HazelcastInstance targetInstance;
    private IMap<Object, Object> map;
    private IList<MapMaxSizeOperationCounter> operationCounterList;
    private int maxSizePerNode;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename);
        operationCounterList = targetInstance.getList(basename + "OperationCounter");

        mapOperationSelectorBuilder
                .addOperation(MapOperation.PUT, putProb)
                .addOperation(MapOperation.GET, getProb)
                .addOperation(MapOperation.CHECK_SIZE, checkProb);

        mapPutOperationSelectorBuilder
                .addOperation(MapPutOperation.PUT_ASYNC, putUsingAsyncProb)
                .addDefaultOperation(MapPutOperation.PUT_SYNC);


        if(isMemberNode(targetInstance)){
            try {
                MaxSizeConfig maxSizeConfig = targetInstance.getConfig().getMapConfig(basename).getMaxSizeConfig();
                maxSizePerNode = maxSizeConfig.getSize();
                assertEqualsStringFormat("Expected MaxSizePolicy %s, but was %s", PER_NODE, maxSizeConfig.getMaxSizePolicy());
                assertTrue("Expected MaxSizePolicy.getSize() < Integer.MAX_VALUE", maxSizePerNode < Integer.MAX_VALUE);

                log.info("MapSizeConfig of " + basename + ": " + maxSizeConfig);
            } catch (Exception e) {
                ExceptionReporter.report(testContext.getTestId(), e);
                throw e;
            }
        }
    }

    @Verify(global = true)
    public void globalVerify() throws Exception {
        MapMaxSizeOperationCounter total = new MapMaxSizeOperationCounter();
        for (MapMaxSizeOperationCounter operationCounter : operationCounterList) {
            total.add(operationCounter);
        }
        log.info(format("Operation counters from %s: %s", basename, total));

        assertMapMaxSize();
    }

    private void assertMapMaxSize() {
        if(isMemberNode(targetInstance)){
            int mapSize = map.size();
            int clusterSize = targetInstance.getCluster().getMembers().size();
            assertTrue(format("Size of map %s should be <= %d * %d, but was %d", basename, clusterSize, maxSizePerNode, mapSize),
                    mapSize <= clusterSize * maxSizePerNode);
        }
    }

    @RunWithWorker
    public AbstractWorker<MapOperation> createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorker<MapOperation> {
        private final MapMaxSizeOperationCounter operationCounter = new MapMaxSizeOperationCounter();
        private final OperationSelector<MapPutOperation> mapPutSelector = mapPutOperationSelectorBuilder.build();

        public Worker() {
            super(mapOperationSelectorBuilder);
        }

        @Override
        public void timeStep(MapOperation operation) {
            final int key = randomInt(keyCount);

            switch (operation) {
                case PUT:
                    final Object value = randomInt();
                    switch (mapPutSelector.select()) {
                        case PUT_SYNC:
                            map.put(key, value);
                            operationCounter.put++;
                            break;
                        case PUT_ASYNC:
                            map.putAsync(key, value);
                            operationCounter.putAsync++;
                            break;
                        default:
                            throw new UnsupportedOperationException();
                    }

                    break;
                case GET:
                    map.get(key);
                    operationCounter.get++;
                    break;
                case CHECK_SIZE:
                    assertMapMaxSize();
                    operationCounter.verified++;
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }

        @Override
        protected void afterRun() {
            operationCounterList.add(operationCounter);
        }
    }
}
