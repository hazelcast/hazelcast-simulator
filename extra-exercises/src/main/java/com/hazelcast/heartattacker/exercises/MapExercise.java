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

import com.hazelcast.core.IMap;
import com.hazelcast.heartattacker.performance.OperationsPerSecond;
import com.hazelcast.heartattacker.performance.Performance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class MapExercise extends AbstractExercise {

    private final static ILogger log = Logger.getLogger(MapExercise.class);

    private final static String alphabet = "abcdefghijklmnopqrstuvwxyz1234567890";

    private IMap<Object, Object> map;
    private String[] keys;
    private String[] values;
    private Random random = new Random();
    private final AtomicLong operations = new AtomicLong();

    //properties
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
        map = hazelcastInstance.getMap(exerciseId + ":Map");
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
                Object value = values[random.nextInt(values.length)];

                if (usePut) {
                    map.put(key, value);
                } else {
                    map.set(key, value);
                }

                if (iteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }

                if(iteration % performanceUpdateFrequency == 0){
                    operations.addAndGet(performanceUpdateFrequency);
                }

                iteration++;
            }
        }
    }


    public static void main(String[] args) throws Exception {
        MapExercise mapExercise = new MapExercise();
        new ExerciseRunner().run(mapExercise, 20);
    }
}
