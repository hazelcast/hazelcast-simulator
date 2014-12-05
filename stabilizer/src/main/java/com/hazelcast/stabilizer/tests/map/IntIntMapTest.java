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
package com.hazelcast.stabilizer.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.probes.probes.IntervalProbe;
import com.hazelcast.stabilizer.probes.probes.SimpleProbe;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.TestRunner;
import com.hazelcast.stabilizer.tests.annotations.*;
import com.hazelcast.stabilizer.tests.map.helpers.KeyUtils;
import com.hazelcast.stabilizer.tests.utils.KeyLocality;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;
import com.hazelcast.stabilizer.worker.OperationSelector;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.stabilizer.tests.map.IntIntMapTest.Operation.*;

public class IntIntMapTest {

    private final static ILogger log = Logger.getLogger(IntIntMapTest.class);

    //props
    public int writePercentage = 10;
    public int threadCount = 10;
    public int keyLength = 10;
    public int valueLength = 10;
    public int keyCount = 10000;
    public int valueCount = 10000;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 10000;
    public String basename = "intIntMap";
    public KeyLocality keyLocality = KeyLocality.Random;
    public int minNumberOfMembers = 0;

	public double putProb = 0.1;
	public double setProb = 0.0;

	//probes
	public IntervalProbe getLatency;
	public IntervalProbe putLatency;
	public SimpleProbe throughput;

    private IMap<Integer, Integer> map;
    private int[] keys;
    private final AtomicLong operations = new AtomicLong();
    private TestContext testContext;

    private HazelcastInstance targetInstance;

	private OperationSelector<Operation> selector = new OperationSelector<Operation>();

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename + "-" + testContext.getTestId());

	    selector.addOperation(PUT, putProb)
	            .addOperation(SET, setProb)
			    .addOperationRemainingProbability(GET);
    }

    @Teardown
    public void teardown() throws Exception {
        map.destroy();
        log.info(TestUtils.getOperationCountInformation(targetInstance));
    }

    @Warmup(global = false)
    public void warmup() throws InterruptedException {
        TestUtils.waitClusterSize(log, targetInstance, minNumberOfMembers);
        keys = KeyUtils.generateIntKeys(keyCount, Integer.MAX_VALUE, keyLocality, testContext.getTargetInstance());

        Random random = new Random();
        for (int key : keys) {
            int value = random.nextInt(Integer.MAX_VALUE);
            map.put(key, value);
        }
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    @Performance
    public long getOperationCount() {
        return operations.get();
    }

    private class Worker implements Runnable {
        private final Random random = new Random();

        @Override
        public void run() {
            long iteration = 0;
            while (!testContext.isStopped()) {

                int key = randomKey();
                int value = randomValue();

	            switch (selector.select()) {
		            case PUT:
			            putLatency.started();
			            map.put(key, value);
			            putLatency.done();
			            break;
		            case SET:
			            putLatency.started();
			            map.put(key, value);
			            putLatency.done();
			            break;
	                case GET:
			            getLatency.started();
			            map.put(key, value);
			            getLatency.done();
			            break;
	            }

                if (iteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }

                if (iteration % performanceUpdateFrequency == 0) {
                    operations.addAndGet(performanceUpdateFrequency);
                }

                iteration++;
                throughput.done();
            }
        }

        private int randomKey() {
            int length = keys.length;
            return keys[random.nextInt(length)];
        }

        private int randomValue() {
            return random.nextInt(Integer.MAX_VALUE);
        }
    }

    public static void main(String[] args) throws Throwable {
        IntIntMapTest test = new IntIntMapTest();
        test.writePercentage = 10;
        new TestRunner<IntIntMapTest>(test).run();
    }

	static enum Operation {
		PUT,
		SET,
		GET
	}
}
