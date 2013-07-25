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

    public void sleepSeconds(int seconds) {
        int period = 30;
        int big = seconds / period;
        int small = seconds % period;

        for (int k = 1; k <= big; k++) {
            Utils.sleepSeconds(period);
            final int elapsed = period * k;
            final float percentage = (100f * elapsed) / seconds;
            String msg = format("Running %s of %s seconds %-4.2f percent complete", elapsed, seconds, percentage);
            log.log(Level.INFO, msg);
        }

        Utils.sleepSeconds(small);
    }

    public void run(Exercise exercise, int durationSec) throws Exception {
        if (hazelcastInstance == null) {
            hazelcastInstance = Hazelcast.newHazelcastInstance(exercise.prepareConfig());
        }

        exercise.setHazelcastInstance(hazelcastInstance);
        exercise.setExerciseId(UUID.randomUUID().toString());

        log.log(Level.INFO, "Starting localSetup");
        exercise.localSetup();
        log.log(Level.INFO, "Finished localSetup");

        log.log(Level.INFO, "Starting globalSetup");
        exercise.globalSetup();
        log.log(Level.INFO, "Finished globalSetup");

        log.log(Level.INFO, "Starting start");
        exercise.start();
        log.log(Level.INFO, "Finished start");

        sleepSeconds(durationSec);

        log.log(Level.INFO, "Starting stop");
        exercise.stop();
        log.log(Level.INFO, "Finished stop");

        log.log(Level.INFO, "Starting globalVerify");
        exercise.globalVerify();
        log.log(Level.INFO, "Finished globalVerify");

        log.log(Level.INFO, "Starting localVerify");
        exercise.localVerify();
        log.log(Level.INFO, "Finished localVerify");

        log.log(Level.INFO, "Starting localTearDown");
        exercise.localTearDown();
        log.log(Level.INFO, "Finished localTearDown");

        log.log(Level.INFO, "Starting globalTearDown");
        exercise.globalTearDown();
        log.log(Level.INFO, "Finished globalTearDown");

        hazelcastInstance.getLifecycleService().shutdown();
        System.out.println("Finished");
    }
}
