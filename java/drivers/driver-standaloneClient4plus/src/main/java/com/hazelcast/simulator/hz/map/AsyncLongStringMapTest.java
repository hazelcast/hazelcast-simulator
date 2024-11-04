package com.hazelcast.simulator.hz.map;

/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.map.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.probes.LatencyProbe;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import com.hazelcast.simulator.worker.testcontainer.Probability;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.simulator.utils.GeneratorUtils.generateAsciiStrings;

/**
 * An IMap test that uses async calls to generate load which is much more efficient
 * than using sync calls. So instead of having a set of threads calling blocking methods,
 * just a few threads are needed that keep a level of concurrent request in flight.
 * <p/>
 * The big advantage is that a client can generate much higher levels of load without
 * becoming the bottleneck itself.
 * <p/>
 * This is a POC implementation. In the future code generation should be used to do the
 * actual work.
 */
public class AsyncLongStringMapTest extends HazelcastTest {

    private final static byte METHOD_GET = 0;
    private final static byte METHOD_SET = 1;
    private final static byte METHOD_PUT = 2;

    // properties
    public int concurrency = 100;
    public long keyDomain = 10000;
    public int valueCount = 10000;
    public int minValueLength = 10;
    public int maxValueLength = 10;
    public boolean fillOnPrepare = true;
    public boolean destroyOnExit = true;
    public double getProb = 1;
    public double putProb = 0;
    public double setProb = 0;

    private IMap<Long, String> map;
    private String[] values;
    private byte[] probabilityArray;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
        values = generateAsciiStrings(valueCount, minValueLength, maxValueLength);

        List<Probability> probabilityList = new ArrayList<>();
        probabilityList.add(new Probability(getProb));
        probabilityList.add(new Probability(putProb));
        probabilityList.add(new Probability(setProb));

        probabilityArray = Probability.loadTimeStepProbabilityArray(probabilityList);
    }

    @Prepare(global = true)
    public void prepare() {
        if (!fillOnPrepare) {
            return;
        }

        Random random = new Random();
        Streamer<Long, String> streamer = StreamerFactory.getInstance(map);
        for (long key = 0; key < keyDomain; key++) {
            String value = values[random.nextInt(valueCount)];
            streamer.pushEntry(key, value);
        }
        streamer.await();
    }

    @Run
    public void run() throws InterruptedException {
        CountDownLatch completionLatch = new CountDownLatch(concurrency);
        for (int k = 0; k < concurrency; k++) {
            Task task = new Task(completionLatch);
            task.run();
        }
        completionLatch.await();
    }

    private class Task implements Runnable {
        private final ThreadState state = new ThreadState();
        private final LatencyProbe getLatencyProbe = testContext.getLatencyProbe("get");
        private final LatencyProbe putLatencyProbe = testContext.getLatencyProbe("put");
        private final LatencyProbe setLatencyProbe = testContext.getLatencyProbe("set");
        private final CountDownLatch completionLatch;
        private long startNs;
        private boolean initializing = true;
        private final ThreadState threadState = new ThreadState();
        private int method;

        private Task(CountDownLatch completionLatch) {
            this.completionLatch = completionLatch;
        }

        @Override
        public void run() {
            if (initializing) {
                initializing = false;
                startNs = System.nanoTime();
            } else {
                long nowNs = System.nanoTime();
                long durationNs = nowNs - startNs;
                switch (method) {
                    case METHOD_GET:
                        getLatencyProbe.recordValue(durationNs);
                        break;
                    case METHOD_SET:
                        setLatencyProbe.recordValue(durationNs);
                        break;
                    case METHOD_PUT:
                        putLatencyProbe.recordValue(durationNs);
                        break;
                    default:
                        throw new RuntimeException("unexpected method index: " + method);
                }

                startNs = nowNs;
            }

            if (testContext.isStopped()) {
                completionLatch.countDown();
                return;
            }

            int probabilityPosition = threadState.randomInt(probabilityArray.length);
            this.method = probabilityArray[probabilityPosition];

            switch (method) {
                case METHOD_GET: {
                    CompletionStage<String> f = map.getAsync(state.randomKey());
                    f.thenRunAsync(this);
                }
                break;
                case METHOD_SET: {
                    CompletionStage<Void> f = map.setAsync(state.randomKey(), threadState.randomValue());
                    f.thenRunAsync(this);
                }
                break;
                case METHOD_PUT: {
                    CompletionStage<String> f = map.putAsync(state.randomKey(), threadState.randomValue());
                    f.thenRunAsync(this);
                }
                break;
                default:
                    throw new RuntimeException("unexpected method index: " + method);
            }
        }
    }

    public class ThreadState extends BaseThreadState {

        private long randomKey() {
            return randomLong(keyDomain);
        }

        private String randomValue() {
            return values[randomInt(values.length)];
        }
    }

    @Teardown
    public void tearDown() {
        if (destroyOnExit) {
            map.destroy();
        }
    }
}
