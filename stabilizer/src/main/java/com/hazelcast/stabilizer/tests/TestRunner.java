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
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class TestRunner {

    private final static ILogger log = Logger.getLogger(TestRunner.class);

    private HazelcastInstance hazelcastInstance;
    private long stopTimeoutMs = TimeUnit.SECONDS.toMillis(60);

    public HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

    public void setHazelcastInstance(HazelcastInstance hz) {
        this.hazelcastInstance = hz;
    }

    public static ILogger getLog() {
        return log;
    }

    public long getStopTimeoutMs() {
        return stopTimeoutMs;
    }

    public void setStopTimeoutMs(long stopTimeoutMs) {
        this.stopTimeoutMs = stopTimeoutMs;
    }

    public void sleepSeconds(Test test, int seconds) {
        int period = 30;
        int big = seconds / period;
        int small = seconds % period;

        for (int k = 1; k <= big; k++) {
            Utils.sleepSeconds(period);
            final int elapsed = period * k;
            final float percentage = (100f * elapsed) / seconds;
            String msg = format("Running %s of %s seconds %-4.2f percent complete", elapsed, seconds, percentage);
            log.info(msg);
            log.info(test.calcPerformance().toHumanString());
        }

        Utils.sleepSeconds(small);
    }

    public void run(Test test, int durationSec) throws Exception {
        if (hazelcastInstance == null) {
            hazelcastInstance = Hazelcast.newHazelcastInstance();
        }

        TestDependencies dependencies = new TestDependencies();
        dependencies.clientInstance = null;
        dependencies.serverInstance = hazelcastInstance;
        dependencies.testId = UUID.randomUUID().toString();

        test.init(dependencies);

        log.info("Starting localSetup");
        test.localSetup();
        log.info("Finished localSetup");

        log.info("Starting globalSetup");
        test.globalSetup();
        log.info("Finished globalSetup");

        log.info("Starting start");
        test.start(false);
        log.info("Finished start");

        sleepSeconds(test, durationSec);

        log.info("Starting stop");
        test.stop(stopTimeoutMs);
        log.info("Finished stop");

        log.info(test.calcPerformance().toHumanString());

        log.info("Starting globalVerify");
        test.globalVerify();
        log.info("Finished globalVerify");

        log.info("Starting localVerify");
        test.localVerify();
        log.info("Finished localVerify");

        log.info("Starting globalTearDown");
        test.globalTearDown();
        log.info("Finished globalTearDown");

        log.info("Starting localTearDown");
        test.localTearDown();
        log.info("Finished localTearDown");

        hazelcastInstance.getLifecycleService().shutdown();
        log.info("Finished");
    }
}
