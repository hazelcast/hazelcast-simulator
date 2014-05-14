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

import java.util.UUID;

import static java.lang.String.format;

public class TestRunner {

    private final static ILogger log = Logger.getLogger(TestRunner.class);
    private final Object test;
    private final TestInvoker testInvoker;

    private HazelcastInstance hazelcastInstance;
    private int duratioSeconds = 60;
    private final  TestContextImpl testContext = new TestContextImpl();

    public TestRunner(Object test) {
        if(test == null){
            throw new NullPointerException();
        }
        this.test = test;
        testInvoker = new TestInvoker(test);

    }

    public TestRunner withHazelcastInstance(HazelcastInstance hz) {
        this.hazelcastInstance = hz;
        return this;
    }

    public TestRunner withDuration(int seconds) {
        this.duratioSeconds = seconds;
        return this;
    }

    public HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

    public long getDuratioSeconds() {
        return duratioSeconds;
    }

    public void run() throws Throwable {
        if (hazelcastInstance == null) {
            hazelcastInstance = Hazelcast.newHazelcastInstance();
        }

        log.info("Starting setup");
        testInvoker.setup(testContext);
        log.info("Finished setup");

        log.info("Starting local warmup");
        testInvoker.warmup(true);
        log.info("Finished local warmup");

        log.info("Starting global warmup");
        testInvoker.warmup(true);
        log.info("Finished global warmup");

        log.info("Starting run");
        new StopThread().run();
        testInvoker.run();
        testContext.stopped=false;
        log.info("Finished start");

        //log.info(test.getOperationCount().toHumanString());

        log.info("Starting globalVerify");
        testInvoker.verify(false);
        log.info("Finished globalVerify");

        log.info("Starting localVerify");
        testInvoker.verify(true);
        log.info("Finished localVerify");

        log.info("Starting globalTearDown");
        testInvoker.teardown(false);
        log.info("Finished globalTearDown");

        log.info("Starting local teardown");
        testInvoker.teardown(true);
        log.info("Finished local teardown");

        hazelcastInstance.getLifecycleService().shutdown();
        log.info("Finished");
    }

    private class StopThread extends Thread {

        public void run() {
            for (int k = 1; k <= duratioSeconds; k++) {
                Utils.sleepSeconds(1);
                final float percentage = (100f * k) / duratioSeconds;
                String msg = format("Running %s of %s seconds %-4.2f percent complete", k, duratioSeconds, percentage);
                log.info(msg);
                //log.info("Performance"+test.getOperationCount());
            }

            testContext.stopped=true;
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
