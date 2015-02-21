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
package com.hazelcast.stabilizer.test;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.stabilizer.probes.probes.ProbesConfiguration;
import com.hazelcast.stabilizer.worker.TestContainer;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.UUID;

import static com.hazelcast.stabilizer.utils.CommonUtils.closeQuietly;
import static com.hazelcast.stabilizer.utils.CommonUtils.sleepSeconds;
import static java.lang.String.format;

/**
 * A utility class to run a test locally. This is purely meant for developing purposes; when you are writing a test
 * you want to see quickly if it works at all without needing to deploy it through an agent on a worker.
 *
 * @param <E>    class of the test
 */
@SuppressWarnings("unused")
public class TestRunner<E> {

    private final static Logger log = Logger.getLogger(TestRunner.class);
    private final E test;
    private final TestContextImpl testContext;
    private final TestContainer testInvoker;

    private HazelcastInstance hazelcastInstance;
    private int durationSeconds = 60;

    public TestRunner(E test) {
        if (test == null) {
            throw new NullPointerException("test can't be null");
        }

        this.test = test;
        this.testContext = new TestContextImpl();
        this.testInvoker = new TestContainer<TestContext>(test, testContext, new ProbesConfiguration());
    }

    public E getTest() {
        return test;
    }

    public HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

    public TestRunner withHazelcastInstance(HazelcastInstance hz) {
        if (hz == null) {
            throw new NullPointerException("hz can't be null");
        }

        this.hazelcastInstance = hz;
        return this;
    }

    public TestRunner withHazelcastConfig(File file) throws IOException {
        if (file == null) {
            throw new NullPointerException("file can't be null");
        }

        if (!file.exists()) {
            throw new IllegalArgumentException(format("file [%s] doesn't exist", file.getAbsolutePath()));
        }

        FileInputStream fis = new FileInputStream(file);
        try {
            Config config = new XmlConfigBuilder(fis).build();
            hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        } finally {
            closeQuietly(fis);
        }

        return this;
    }

    public TestRunner withDuration(int durationSeconds) {
        if (durationSeconds < 0) {
            throw new IllegalArgumentException("Duration can't be smaller than 0");
        }

        this.durationSeconds = durationSeconds;
        return this;
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
        log.info("Finished run");

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

        //hazelcastInstance.shutdown();
        log.info("Finished");
    }

    private class StopThread extends Thread {
        @Override
        public void run() {
            int period = 5;
            int big = durationSeconds / period;
            int small = durationSeconds % period;

            for (int k = 1; k <= big; k++) {
                sleepSeconds(period);
                final int elapsed = period * k;
                final float percentage = (100f * elapsed) / durationSeconds;
                String msg = format("Running %s of %s seconds %-4.2f percent complete", elapsed, durationSeconds, percentage);
                log.info(msg);
                //log.info("Performance" + test.getOperationCount());
            }

            sleepSeconds(small);
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

        @Override
        public void stop() {
            throw new UnsupportedOperationException("Not implemented");
        }
    }
}
