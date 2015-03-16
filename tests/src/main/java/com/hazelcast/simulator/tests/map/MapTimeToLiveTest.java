/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.simulator.test.utils.AssertTask;
import com.hazelcast.simulator.tests.map.helpers.MapOperationCounter;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.utils.ThreadSpawner;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.test.utils.TestUtils.assertTrueEventually;
import static org.junit.Assert.assertEquals;

/**
 * In this test we are using map put methods with an Expire time.
 * we put keys at random into the map using sync and async methods with some proablity distribution
 * in the end we verify that the map is empty and all key value pairs have expired out of the map
 */
public class MapTimeToLiveTest {

    private static final ILogger log = Logger.getLogger(MapTimeToLiveTest.class);

    // properties
    public String basename = this.getClass().getSimpleName();
    public int threadCount = 3;
    public int keyCount = 10;

    //check these add up to 1
    public double putTTLProb = 0.4;
    public double putAsyncTTLProb = 0.3;
    public double getProb = 0.2;
    public double getAsyncProb = 0.1;
    public double destroyProb = 0.0;

    public int maxTTLExpireyMs = 3000;
    public int minTTLExpireyMs = 1;

    private TestContext testContext;
    private HazelcastInstance targetInstance;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    private class Worker implements Runnable {
        private MapOperationCounter count = new MapOperationCounter();
        private final Random random = new Random();

        @Override
        public void run() {
            while (!testContext.isStopped()) {
                try {
                    final int key = random.nextInt(keyCount);
                    final IMap map = targetInstance.getMap(basename);

                    double chance = random.nextDouble();
                    if ((chance -= putTTLProb) < 0) {
                        final Object value = random.nextInt();
                        int delayMs = minTTLExpireyMs + random.nextInt(maxTTLExpireyMs);
                        map.put(key, value, delayMs, TimeUnit.MILLISECONDS);
                        count.putTTLCount.incrementAndGet();
                    } else if ((chance -= putAsyncTTLProb) < 0) {
                        final Object value = random.nextInt();
                        int delayMs = minTTLExpireyMs + random.nextInt(maxTTLExpireyMs);
                        map.putAsync(key, value, delayMs, TimeUnit.MILLISECONDS);
                        count.putAsyncTTLCount.incrementAndGet();
                    } else if ((chance -= getProb) < 0) {
                        map.get(key);
                        count.getCount.incrementAndGet();
                    } else if ((chance -= getAsyncProb) < 0) {
                        map.getAsync(key);
                        count.getAsyncCount.incrementAndGet();
                    } else if ((chance -= destroyProb) <= 0) {
                        map.destroy();
                        count.destroyCount.incrementAndGet();
                    }

                } catch (DistributedObjectDestroyedException e) {
                }
            }
            IList<MapOperationCounter> results = targetInstance.getList(basename + "report");
            results.add(count);
        }
    }

    @Verify(global = true)
    public void globalVerify() throws Exception {

        IList<MapOperationCounter> results = targetInstance.getList(basename + "report");
        MapOperationCounter total = new MapOperationCounter();
        for (MapOperationCounter i : results) {
            total.add(i);
        }
        log.info(basename + ": " + total + " total of " + results.size());

        final IMap map = targetInstance.getMap(basename);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(basename + ": Map Size not 0, some TTL events not processed", 0, map.size());
            }
        });
    }


    public static void main(String[] args) throws Throwable {
        new TestRunner<MapAsyncOpsTest>(new MapAsyncOpsTest()).run();
    }
}
