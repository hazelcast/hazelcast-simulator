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
package com.hazelcast.heartattacker.exercises;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.heartattacker.Utils;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.apache.log4j.lf5.LogLevel;

import java.util.UUID;
import java.util.logging.Level;

import static java.lang.String.format;

public class ExerciseRunner {

    private final static ILogger log = Logger.getLogger(ExerciseRunner.class);

    private HazelcastInstance hazelcastInstance;

    public HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

    public void setHazelcastInstance(HazelcastInstance hz) {
        this.hazelcastInstance = hz;
    }

    public void sleepSeconds(Exercise exercise, int seconds) {
        int period = 30;
        int big = seconds / period;
        int small = seconds % period;

        for (int k = 1; k <= big; k++) {
            Utils.sleepSeconds(period);
            final int elapsed = period * k;
            final float percentage = (100f * elapsed) / seconds;
            String msg = format("Running %s of %s seconds %-4.2f percent complete", elapsed, seconds, percentage);
            log.info(msg);
            log.info(exercise.calcPerformance().toHumanString());
        }

        Utils.sleepSeconds(small);
    }

    public void run(Exercise exercise, int durationSec) throws Exception {
        if (hazelcastInstance == null) {
            hazelcastInstance = Hazelcast.newHazelcastInstance();
        }

        exercise.setHazelcastInstance(hazelcastInstance);
        exercise.setExerciseId(UUID.randomUUID().toString());

        log.info("Starting localSetup");
        exercise.localSetup();
        log.info( "Finished localSetup");

        log.info( "Starting globalSetup");
        exercise.globalSetup();
        log.info( "Finished globalSetup");

        log.info("Starting start");
        exercise.start();
        log.info( "Finished start");

        sleepSeconds(exercise, durationSec);

        log.info("Starting stop");
        exercise.stop();
        log.info("Finished stop");

        log.info(exercise.calcPerformance().toHumanString());

        log.info("Starting globalVerify");
        exercise.globalVerify();
        log.info("Finished globalVerify");

        log.info("Starting localVerify");
        exercise.localVerify();
        log.info( "Finished localVerify");

        log.info("Starting localTearDown");
        exercise.localTearDown();
        log.info( "Finished localTearDown");

        log.info( "Starting globalTearDown");
        exercise.globalTearDown();
        log.info("Finished globalTearDown");

        hazelcastInstance.getLifecycleService().shutdown();
        log.info("Finished");
    }
}
