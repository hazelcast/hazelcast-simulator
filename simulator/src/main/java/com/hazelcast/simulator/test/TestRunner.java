/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.test;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.worker.TestContainer;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.CommonUtils.joinThread;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.PropertyBindingSupport.bindProperties;
import static java.lang.String.format;

/**
 * A utility class to run a test locally.
 *
 * This is purely meant for developing purposes, e.g. when you are writing a test and you want to see quickly if it works at all
 * without needing to deploy it through an agent on a worker.
 *
 * @param <E> class of the test
 */
public class TestRunner<E> {

    private static final int DEFAULT_DURATION_SECONDS = 60;

    private static final Logger LOGGER = Logger.getLogger(TestRunner.class);

    private final StopThread stopThread = new StopThread();
    private final TestContextImpl testContext = new TestContextImpl();

    private final TestContainer testInvoker;
    private final E test;

    private int durationSeconds = DEFAULT_DURATION_SECONDS;
    private HazelcastInstance hazelcastInstance;

    public TestRunner(E test) {
        this(test, null);
    }

    public TestRunner(E test, Map<String, String> properties) {
        if (test == null) {
            throw new NullPointerException("test can't be null");
        }

        TestCase testCase = null;
        if (properties != null) {
            testCase = new TestCase("TestRunner", properties);
            bindProperties(test, testCase, null);
        }

        this.testInvoker = new TestContainer(test, testContext, testCase);
        this.test = test;
    }

    public E getTest() {
        return test;
    }

    public long getDurationSeconds() {
        return durationSeconds;
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

    public TestRunner withHazelcastConfigFile(File file) throws IOException {
        if (file == null) {
            throw new NullPointerException("file can't be null");
        }
        if (!file.exists()) {
            throw new IllegalArgumentException(format("file [%s] doesn't exist", file.getAbsolutePath()));
        }

        FileInputStream inputStream = new FileInputStream(file);
        try {
            Config config = new XmlConfigBuilder(inputStream).build();
            hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        } finally {
            closeQuietly(inputStream);
        }

        return this;
    }

    public TestRunner withHazelcastConfig(Config config) {
        if (config == null) {
            throw new NullPointerException("config can't be null");
        }

        hazelcastInstance = Hazelcast.newHazelcastInstance(config);

        return this;
    }

    public TestRunner withDuration(int durationSeconds) {
        if (durationSeconds < 0) {
            throw new IllegalArgumentException("durationSeconds can't be smaller than 0");
        }

        this.durationSeconds = durationSeconds;
        return this;
    }

    TestRunner withSleepInterval(int sleepInterval) {
        if (sleepInterval < 1) {
            throw new IllegalArgumentException("sleepInterval can't be smaller than 1");
        }

        stopThread.defaultSleepInterval = sleepInterval;
        return this;
    }

    public void run() throws Exception {
        try {
            if (hazelcastInstance == null) {
                hazelcastInstance = Hazelcast.newHazelcastInstance();
            }

            runPhase(TestPhase.SETUP);

            runPhase(TestPhase.LOCAL_WARMUP);
            runPhase(TestPhase.GLOBAL_WARMUP);

            LOGGER.info("Starting run");
            stopThread.start();
            testInvoker.invoke(TestPhase.RUN);
            LOGGER.info("Finished run");

            runPhase(TestPhase.GLOBAL_VERIFY);
            runPhase(TestPhase.LOCAL_VERIFY);

            runPhase(TestPhase.GLOBAL_TEARDOWN);
            runPhase(TestPhase.LOCAL_TEARDOWN);
        } finally {
            LOGGER.info("Shutdown...");
            if (hazelcastInstance != null) {
                hazelcastInstance.shutdown();
            }

            stopThread.interrupt();
            joinThread(stopThread);
            LOGGER.info("Finished");
        }
    }

    private void runPhase(TestPhase testPhase) throws Exception {
        LOGGER.info("Starting " + testPhase.desc());
        testInvoker.invoke(testPhase);
        LOGGER.info("Finished " + testPhase.desc());
    }

    private final class StopThread extends Thread {

        private static final int DEFAULT_SLEEP_INTERVAL = 5;
        private static final float ONE_HUNDRED = 100f;

        private volatile int defaultSleepInterval = DEFAULT_SLEEP_INTERVAL;

        @Override
        public void run() {
            testContext.stopped = false;

            int sleepInterval = defaultSleepInterval;
            int sleepIterations = durationSeconds / sleepInterval;
            for (int i = 1; i <= sleepIterations; i++) {
                sleepSeconds(sleepInterval);

                int elapsed = i * sleepInterval;
                float percentage = elapsed * ONE_HUNDRED / durationSeconds;
                LOGGER.info(format("Running %d of %d seconds %-4.2f percent complete", elapsed, durationSeconds, percentage));
            }

            sleepSeconds(durationSeconds % sleepInterval);
            testContext.stop();
            LOGGER.info("Notified test to stop");
        }
    }

    private final class TestContextImpl implements TestContext {

        private final String testId = UUID.randomUUID().toString();

        private volatile boolean stopped;

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
            stopped = true;
        }
    }
}
