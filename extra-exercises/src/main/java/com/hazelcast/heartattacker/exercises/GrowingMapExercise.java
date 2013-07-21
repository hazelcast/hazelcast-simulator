package com.hazelcast.heartattacker.exercises;

import com.hazelcast.core.IMap;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.logging.Level;

public class GrowingMapExercise extends AbstractExercise {

    private final static ILogger log = Logger.getLogger(GrowingMapExercise.class);

    private IMap<Long, String> map;
    private IdGenerator idGenerator;

    public int threadCount = 10;
    public int growCount = 10000;
    public boolean usePut = true;
    public boolean useRemove = true;
    public int valueSize = 10;
    public int logFrequency = 10000;
    public boolean removeOnStop = true;

    private String value;

    @Override
    public void localSetup() throws Exception {
        idGenerator = hazelcastInstance.getIdGenerator(exerciseId + ":IdGenerator");
        map = hazelcastInstance.getMap(exerciseId + ":Map");
        for (int k = 0; k < threadCount; k++) {
            spawn(new Worker());
        }

        value = buildValue();
    }

    private String buildValue() {
        StringBuffer sb = new StringBuffer();
        for (int k = 0; k < valueSize; k++) {
            sb.append(' ');
        }
        return sb.toString();
    }

    @Override
    public void globalTearDown() throws Exception {
        map.destroy();
    }

    @Override
    public void globalVerify() throws Exception {
        if (removeOnStop && !map.isEmpty()) {
            throw new RuntimeException("Map should be empty, but has size:" + map.size());
        }
    }

    private class Worker implements Runnable {
        @Override
        public void run() {
            long insertIteration = 0;
            long deleteIteration = 0;

            long[] keys = new long[growCount];

            while (!stop) {
                int keyIndex=-1;
                for (int k = 0; k < growCount; k++) {
                    if (stop) {
                        break;
                    }

                    long key = idGenerator.newId();
                    keyIndex=k;
                    keys[keyIndex] = key;

                    if (usePut) {
                        map.put(key, value);
                    } else {
                        map.set(key, value);
                    }

                    insertIteration++;
                    if (insertIteration % logFrequency == 0) {
                        log.log(Level.INFO, Thread.currentThread().getName() + " At insert iteration: " + insertIteration);
                    }
                }

                for (int k = 0; k <= keyIndex; k++) {
                    if (stop && !removeOnStop) {
                        break;
                    }

                    long key = keys[k];
                    keys[k] = 0;

                    if (useRemove) {
                        map.remove(key);
                    } else {
                        map.delete(key);
                    }

                    deleteIteration++;
                    if (deleteIteration % logFrequency == 0) {
                        log.log(Level.INFO, Thread.currentThread().getName() + " At delete iteration: " + deleteIteration);
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        GrowingMapExercise mapExercise = new GrowingMapExercise();
        new ExerciseRunner().run(mapExercise, 20);
    }
}
