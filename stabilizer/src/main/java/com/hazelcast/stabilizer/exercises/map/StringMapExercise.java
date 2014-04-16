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
package com.hazelcast.stabilizer.exercises.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.exercises.AbstractExercise;
import com.hazelcast.stabilizer.exercises.ExerciseRunner;
import com.hazelcast.stabilizer.performance.OperationsPerSecond;
import com.hazelcast.stabilizer.performance.Performance;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class StringMapExercise extends AbstractExercise {

    private final static ILogger log = Logger.getLogger(StringMapExercise.class);

    private final static String alphabet = "abcdefghijklmnopqrstuvwxyz1234567890";

    private IMap<Object, Object> map;
    private String[] keys;
    private String[] values;
    private Random random = new Random();
    private final AtomicLong operations = new AtomicLong();

    //properties
    public int writePercentage = 10;
    public int threadCount = 10;
    public int keyLength = 10;
    public int valueLength = 10;
    public int keyCount = 10000;
    public int valueCount = 10000;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 10000;
    public boolean usePut = true;

    @Override
    public void localSetup() throws Exception {
        super.localSetup();

        if (writePercentage < 0) {
            throw new IllegalArgumentException("Write percentage can't be smaller than 0");
        }

        if (writePercentage > 100) {
            throw new IllegalArgumentException("Write percentage can't be larger than 100");
        }

        HazelcastInstance targetInstance = getTargetInstance();

        map = targetInstance.getMap("Map-" + exerciseId);
        for (int k = 0; k < threadCount; k++) {
            spawn(new Worker());
        }

        keys = new String[keyCount];
        for (int k = 0; k < keys.length; k++) {
            keys[k] = makeString(keyLength);
        }

        values = new String[valueCount];
        for (int k = 0; k < values.length; k++) {
            values[k] = makeString(valueLength);
        }

        //if our threads are not going to do any writes, we must fill the map so that a read is possible. Otherwise
        //the map remains empty.
        if (writePercentage == 0) {
            Random random = new Random();
            for (int k = 0; k < keys.length; k++) {
                String key = keys[random.nextInt(keyCount)];
                String value = values[random.nextInt(valueCount)];
                map.put(key, value);
            }
        }
    }

    private String makeString(int length) {
        StringBuilder sb = new StringBuilder();
        for (int k = 0; k < length; k++) {
            char c = alphabet.charAt(random.nextInt(alphabet.length()));
            sb.append(c);
        }

        return sb.toString();
    }

    @Override
    public void globalTearDown() throws Exception {
        map.destroy();
    }

    @Override
    public Performance calcPerformance() {
        OperationsPerSecond performance = new OperationsPerSecond();
        performance.setStartMs(getStartTimeMs());
        performance.setEndMs(getCurrentTimeMs());
        performance.setOperations(operations.get());
        return performance;
    }

    private class Worker implements Runnable {
        private final Random random = new Random();

        @Override
        public void run() {
            long iteration = 0;
            while (!stop) {
                Object key = keys[random.nextInt(keys.length)];

                if (shouldWrite(iteration)) {
                    Object value = values[random.nextInt(values.length)];
                    if (usePut) {
                        map.put(key, value);
                    } else {
                        map.set(key, value);
                    }
                } else {
                    map.get(key);
                }

                if (iteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }

                if (iteration % performanceUpdateFrequency == 0) {
                    operations.addAndGet(performanceUpdateFrequency);
                }

                iteration++;
            }
        }

        private boolean shouldWrite(long iteration) {
            if (writePercentage == 0) {
                return false;
            } else if (writePercentage == 100) {
                return true;
            } else {
                return (iteration % 100) < writePercentage;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        StringMapExercise mapExercise = new StringMapExercise();
        mapExercise.useClient = true;
        mapExercise.writePercentage = 0;
        new ExerciseRunner().run(mapExercise, 20);
    }
}
