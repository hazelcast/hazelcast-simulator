package com.hazelcast.heartattack.exercises;

import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.Random;
import java.util.logging.Level;

public class MapExercise extends AbstractExercise {

    private final static ILogger log = Logger.getLogger(MapExercise.class);

    private final static String alphabet = "abcdefghijklmnopqrstuvwxyz1234567890";

    private IMap<Object, Object> map;
    private String[] keys;
    private String[] values;
    private Random random = new Random();

    public int threadCount = 10;
    private int keyLength = 10;
    private int valueLength = 10;
    private int keyCount = 10000;
    private int valueCount = 10000;

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

    private class Worker implements Runnable {
        private final Random random = new Random();

        @Override
        public void run() {
            long iteration = 0;
            while (!stop) {
                Object key = keys[random.nextInt(keys.length)];
                Object value = values[random.nextInt(values.length)];
                map.put(key, value);
                if (iteration % 10000 == 0) {
                    log.log(Level.INFO, Thread.currentThread().getName() + " At iteration: " + iteration);
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
