package com.hazelcast.heartattack.exercises;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.heartattack.Utils.writeObject;

public class ExerciseUtils {
    public final static AtomicLong HEART_ATTACK_ID = new AtomicLong(1);

    public static String getTraineeId() {
        return System.getProperty("traineeId");
    }

    public static void signalHeartAttack(Throwable cause) {
        final File file = new File(getTraineeId() + "." + HEART_ATTACK_ID.incrementAndGet() + ".exception");
        writeObject(cause, file);
    }

    private ExerciseUtils() {
    }
}
