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
package com.hazelcast.simulator.tests.icache;

import com.hazelcast.cache.ICache;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.BeforeRun;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.utils.ThrottlingLogger;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntKeys;
import static com.hazelcast.simulator.tests.icache.helpers.CacheUtils.getCache;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.GeneratorUtils.generateByteArray;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * An ICache test with Integer as key and a byte-array as value.
 *
 * It makes use of the circuit breaker patter for puts/gets. If put or get times out, an error is recorded.
 */
public class IntBytesICacheCircuitBreakerTest extends HazelcastTest {

    // properties
    public int keyCount = 1000;
    public int valueCount = 1000;
    public int valueMinSize = 16;
    public int valueMaxSize = 2000;
    // the number of keys that are going to be written.
    // normally you want to keep this the same as keyCount (for reading), but it can help to expose certain problems like
    // gc. If they writeKeyCount is very small, only a small group of objects get updated frequently and helps to prevent
    // getting them tenured. If writeKeyCount is -1, it will automatically be set to keyCount
    public int writeKeyCount = -1;
    public KeyLocality keyLocality = KeyLocality.SHARED;
    // timeout in milliseconds for Cache asynchronous operations
    public int timeoutMs = 50;

    // the backoff in millies between retrying a failed get/put. If this value is set to a very low value, it could
    // lead to a high rate of spinning on failing gets/puts and could result in a high error rate.
    public int backoffMs = 50;

    // if true, any occurrence of TimeoutException is caught and only logged, if false it's propagated further
    public boolean catchTimeoutException = false;
    // number of millisecond for stalling the partition thread using stallPartitionThread method
    public int stallTimeMs = 0;

    private final ThrottlingLogger throttlingLogger = ThrottlingLogger.newLogger(logger, 5000);
    private final StallingEntryProcessor stallingEntryProcessor = new StallingEntryProcessor(stallTimeMs);

    private final AtomicLong puts = new AtomicLong();
    private final AtomicLong gets = new AtomicLong();
    private final AtomicLong putErrors = new AtomicLong();
    private final AtomicLong getErrors = new AtomicLong();
    private IAtomicLong globalPuts;
    private IAtomicLong globalGets;
    private IAtomicLong globalPutErrors;
    private IAtomicLong globalGetErrors;

    private ICache<Integer, Object> cache;
    private int[] keys;
    private byte[][] values;

    @Setup
    public void setUp() {
        cache = getCache(targetInstance, name);
        keys = generateIntKeys(keyCount, keyLocality, targetInstance);

        if (valueMinSize > valueMaxSize) {
            throw new IllegalStateException("valueMinSize can't be larger than valueMaxSize");
        }

        if (writeKeyCount == -1) {
            writeKeyCount = keyCount;
        }

        globalGetErrors = targetInstance.getAtomicLong("globalGetErrors");
        globalPutErrors = targetInstance.getAtomicLong("globalPutErrors");
        globalGets = targetInstance.getAtomicLong("globalGets");
        globalPuts = targetInstance.getAtomicLong("globalPuts");
    }

    @Prepare
    public void prepare() {
        Random random = new Random();
        values = new byte[valueCount][];
        for (int i = 0; i < values.length; i++) {
            int delta = valueMaxSize - valueMinSize;
            int length = delta == 0 ? valueMinSize : valueMinSize + random.nextInt(delta);
            values[i] = generateByteArray(random, length);
        }

        Streamer<Integer, Object> streamer = StreamerFactory.getInstance(cache);
        for (int key : keys) {
            streamer.pushEntry(key, values[random.nextInt(values.length)]);
        }
        streamer.await();
    }

    @BeforeRun
    public void beforeRun() {
        new PublishThread().start();
    }

    @TimeStep(prob = 0.1)
    public void put(ThreadState state) throws Exception {
        puts.incrementAndGet();

        try {
            Future<Void> future = cache.putAsync(state.randomKey(), state.randomValue());
            future.get(timeoutMs, MILLISECONDS);
        } catch (Exception ex) {
            sleepMillis(backoffMs);

            putErrors.incrementAndGet();
            if (catchTimeoutException) {
                throttlingLogger.warn("putAsync didn't finish within configured " + timeoutMs + " ms timeout. "
                        + "Number of timeouts occurred: " + putErrors + " exception:" + ex.getClass());
            } else {
                throw ex;
            }
        }
    }

    @TimeStep(prob = -1)
    public Object get(ThreadState state) throws Exception {
        gets.incrementAndGet();

        Object result = null;
        try {
            Future<Object> future = cache.getAsync(state.randomKey());
            result = future.get(timeoutMs, MILLISECONDS);
        } catch (Exception ex) {
            sleepMillis(backoffMs);
            getErrors.incrementAndGet();
            if (catchTimeoutException) {
                throttlingLogger.warn("getAsync didn't finish within configured " + timeoutMs + " ms timeout. "
                        + "Number of timeouts occurred: " + putErrors + " exception:" + ex.getClass());
            } else {
                throw ex;
            }
        }

        return result;
    }

    @TimeStep(prob = 0)
    public void stall(ThreadState state) {
        cache.invoke(state.randomKey(), stallingEntryProcessor);
    }

    public class ThreadState extends BaseThreadState {

        private int randomKey() {
            return keys[randomInt(keys.length)];
        }

        private byte[] randomValue() {
            return values[randomInt(values.length)];
        }
    }

    static class StallingEntryProcessor implements EntryProcessor<Integer, Object, Object>, Serializable {

        private int stallTime;

        public StallingEntryProcessor(int stallTime) {
            this.stallTime = stallTime;
        }

        @Override
        public Object process(MutableEntry entry, Object... arguments) throws EntryProcessorException {
            try {
                Thread.sleep(stallTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }

            return entry;
        }
    }

    private final class PublishThread extends Thread {
        // only 1 driver should be echoing the fail ratio's.
        private final boolean isEchoer;

        PublishThread() {
            this.isEchoer = targetInstance.getAtomicLong("isEchoer").getAndIncrement() == 0;
        }

        @Override
        public void run() {
            try {
                int k = 0;
                while (!testContext.isStopped()) {
                    k++;
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        continue;
                    }

                    long getFails = globalGetErrors.addAndGet(getErrors.getAndSet(0));
                    long putFails = globalPutErrors.addAndGet(putErrors.getAndSet(0));

                    long gets = globalGets.addAndGet(IntBytesICacheCircuitBreakerTest.this.gets.getAndSet(0));
                    long puts = globalPuts.addAndGet(IntBytesICacheCircuitBreakerTest.this.puts.getAndSet(0));

                    if (isEchoer && k % 5 == 0) {
                        testContext.echoCoordinator("get fail " + ((100d * getFails) / gets) + " %% "
                                + "put fail " + ((100d * putFails) / puts) + " %%");
                    }
                }
            } catch (Exception e) {
                ExceptionReporter.report(testContext.getTestId(), e);
            }
        }
    }

    @Teardown
    public void tearDown() {
        throttlingLogger.info("Total number of TimeoutExceptions occurred: " + putErrors);
        cache.destroy();
    }
}
