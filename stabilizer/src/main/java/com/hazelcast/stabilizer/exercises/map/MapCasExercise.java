package com.hazelcast.stabilizer.exercises.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.exercises.AbstractExercise;
import com.hazelcast.stabilizer.exercises.ExerciseRunner;
import com.hazelcast.stabilizer.performance.OperationsPerSecond;
import com.hazelcast.stabilizer.performance.Performance;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This exercise test the cas method: replace. So for optimistic concurrency control.
 *
 * We have a bunch of predefined keys, and we are going to concurrently increment the value
 * and we protect ourselves against lost updates using cas method replace.
 *
 * Locally we keep track of all increments, and if the sum of these local increments matches the
 * global increment, we are done
 */
public class MapCasExercise extends AbstractExercise {

    private final static ILogger log = Logger.getLogger(MapCasExercise.class);

    private IMap<Integer, Long> map;
    private final AtomicLong operations = new AtomicLong();

    //properties
    public int threadCount = 10;
    public int keyCount = 1000;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 10000;
    private IMap<String, Map<Integer, Long>> resultsPerWorker;

    @Override
    public void localSetup() throws Exception {
        super.localSetup();

        HazelcastInstance targetInstance = getTargetInstance();

        map = targetInstance.getMap("Map-" + exerciseId);
        for (int k = 0; k < threadCount; k++) {
            spawn(new Worker());
        }

        resultsPerWorker = targetInstance.getMap("ResultMap" + exerciseId);
    }

    @Override
    public void globalSetup() throws Exception {
        super.globalSetup();

        for (int k = 0; k < keyCount; k++) {
            map.put(k, 0l);
        }
    }

    @Override
    public void globalTearDown() throws Exception {
        map.destroy();
        resultsPerWorker.destroy();
    }

    @Override
    public void globalVerify() throws Exception {
        long[] amount = new long[keyCount];

        for (Map<Integer, Long> map : resultsPerWorker.values()) {
            for (Map.Entry<Integer, Long> entry : map.entrySet()) {
                amount[entry.getKey()] += entry.getValue();
            }
        }

        int failures = 0;
        for (int k = 0; k < keyCount; k++) {
            long expected = amount[k];
            long found = map.get(k);
            if(expected!=found){
                failures++;
            }
        }

        if(failures>0){
            throw new IllegalStateException("Failures found:"+failures);
        }
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
        private final Map<Integer, Long> result = new HashMap<Integer, Long>();

        @Override
        public void run() {
            for (int k = 0; k < keyCount; k++) {
                result.put(k, 0L);
            }

            long iteration = 0;
            while (!stop) {
                Integer key = random.nextInt(keyCount);
                long increment = random.nextInt(100);

                for (; ; ) {
                    Long current = map.get(key);
                    Long update = current + increment;
                    if (map.replace(key, current, update)) {
                        increment(key, increment);
                        break;
                    }
                }

                if (iteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }

                if (iteration % performanceUpdateFrequency == 0) {
                    operations.addAndGet(performanceUpdateFrequency);
                }

                iteration++;
            }

            resultsPerWorker.put(UUID.randomUUID().toString(), result);
        }

        private void increment(int key, long increment) {
            result.put(key, result.get(key) + increment);
        }
    }

    public static void main(String[] args) throws Exception {
        MapCasExercise mapExercise = new MapCasExercise();
        mapExercise.useClient = true;
        new ExerciseRunner().run(mapExercise, 20);
    }
}
