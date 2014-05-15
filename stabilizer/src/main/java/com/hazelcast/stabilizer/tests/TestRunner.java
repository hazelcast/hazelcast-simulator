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
package com.hazelcast.stabilizer.tests;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.tests.utils.TestInvoker;

import java.util.UUID;

import static java.lang.String.format;

public class TestRunner {

    private final static ILogger log = Logger.getLogger(TestRunner.class);
    private final Object test;
    private final TestInvoker testInvoker;

    private HazelcastInstance hazelcastInstance;
    private int durationSeconds = 60;
    private final TestContextImpl testContext = new TestContextImpl();

    public TestRunner(Object test) {
        if (test == null) {
            throw new NullPointerException("test can't be null");
        }
        this.test = test;
        this.testInvoker = new TestInvoker(test, testContext);
    }

    public TestRunner withHazelcastInstance(HazelcastInstance hz) {
        if (hz == null) {
            throw new NullPointerException("hz can't be null");
        }
        this.hazelcastInstance = hz;
        return this;
    }

    public TestRunner withDuration(int durationSeconds) {
        if (durationSeconds < 0) {
            throw new IllegalArgumentException("Duration can't be smaller than 0");
        }
        this.durationSeconds = durationSeconds;
        return this;
    }

    public HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

    public long getDurationSeconds() {
        return durationSeconds;
    }

    public void run() throws Throwable {
        if (hazelcastInstance == null) {
            hazelcastInstance = Hazelcast.newHazelcastInstance();
        }

        log.info("Starting setup");
        testInvoker.setup();
        log.info("Finished setup");

        log.info("Starting local warmup");
        testInvoker.localWarmup();
        log.info("Finished local warmup");

        log.info("Starting global warmup");
        testInvoker.globalWarmup();
        log.info("Finished global warmup");

        log.info("Starting run");
        testContext.stopped = false;
        new StopThread().start();
        testInvoker.run();
        log.info("Finshed run");

        //log.info(test.getOperationCount().toHumanString());

        log.info("Starting globalVerify");
        testInvoker.globalVerify();
        log.info("Finished globalVerify");

        log.info("Starting localVerify");
        testInvoker.localVerify();
        log.info("Finished localVerify");

        log.info("Starting globalTearDown");
        testInvoker.globalTeardown();
        log.info("Finished globalTearDown");

        log.info("Starting local teardown");
        testInvoker.localTeardown();
        log.info("Finished local teardown");

        hazelcastInstance.shutdown();
        log.info("Finished");
    }

    private class StopThread extends Thread {

        @Override
        public void run() {
            int period = 5;
            int big = durationSeconds / period;
            int small = durationSeconds % period;

            for (int k = 1; k <= big; k++) {
                Utils.sleepSeconds(period);
                final int elapsed = period * k;
                final float percentage = (100f * elapsed) / durationSeconds;
                String msg = format("Running %s of %s seconds %-4.2f percent complete", elapsed, durationSeconds, percentage);
                log.info(msg);
                //log.info("Performance"+test.getOperationCount());
            }


            Utils.sleepSeconds(small);
            log.info("Notified test to stop");
            testContext.stopped = true;
        }
    }

    private class TestContextImpl implements TestContext {
        final String testId = UUID.randomUUID().toString();
        volatile boolean stopped;

        @Override
        public HazelcastInstance getTargetInstance() {
            return hazelcastInstance;
        }

        @Override
        public String getTestId() {
            return testId;
        }

        @Override
        public boolean isStopped() {
            return stopped;
        }
    }
}
