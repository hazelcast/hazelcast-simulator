package com.hazelcast.heartattack.tasks;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.heartattack.ExerciseRecipe;
import com.hazelcast.heartattack.exercises.Exercise;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.logging.Level;

import static java.lang.String.format;

public class InitExercise implements Callable, Serializable, HazelcastInstanceAware {
    private final static ILogger log = Logger.getLogger(InitExercise.class);

    private transient HazelcastInstance hz;
    private final ExerciseRecipe exerciseRecipe;

    public InitExercise(ExerciseRecipe exerciseRecipe) {
        this.exerciseRecipe = exerciseRecipe;
    }

    @Override
    public Object call() throws Exception {
        try {
            log.log(Level.INFO, "Init Exercise");
            String clazzName = exerciseRecipe.getClassname();

            Exercise exercise = (Exercise) InitExercise.class.getClassLoader().loadClass(clazzName).newInstance();
            exercise.setHazelcastInstance(hz);
            exercise.setExerciseId(exerciseRecipe.getExerciseId());

            bindProperties(exercise);

            hz.getUserContext().put(Exercise.EXERCISE_INSTANCE, exercise);
            return null;
        } catch (Exception e) {
            log.log(Level.SEVERE, "Failed to init Exercise", e);
            throw e;
        }
    }

    private void bindProperties(Exercise exercise) throws NoSuchFieldException, IllegalAccessException {
        for (Map.Entry<String, String> entry : exerciseRecipe.getProperties().entrySet()) {
            String property = entry.getKey();
            if ("class".equals(property)) {
                continue;
            }
            String value = entry.getValue();
            bindProperty(exercise, property, value);
        }
    }

    private void bindProperty(Exercise exercise, String property, String value) throws IllegalAccessException {
        Field field = findField(exercise.getClass(), property);
        if (field == null) {
            throw new RuntimeException(format("Could not found a field for property [%s] on class [%s]", property, exercise.getClass()));
        }
        field.setAccessible(true);
        try {
            if (Boolean.class.equals(field.getType()) || Boolean.TYPE.equals(field.getType())) {
                field.set(exercise, Boolean.parseBoolean(value));
            } else if (String.class.equals(field.getType())) {
                field.set(exercise, value);
            } else if (Integer.class.equals(field.getType()) || Integer.TYPE.equals(field.getType())) {
                field.set(exercise, Integer.parseInt(value));
            } else if (Long.class.equals(field.getType()) || Long.TYPE.equals(field.getType())) {
                field.set(exercise, Long.parseLong(value));
            } else if (Float.class.equals(field.getType()) || Float.TYPE.equals(field.getType())) {
                field.set(exercise, Float.parseFloat(value));
            } else if (Double.class.equals(field.getType()) || Double.TYPE.equals(field.getType())) {
                field.set(exercise, Double.parseDouble(value));
            } else {
                throw new RuntimeException(format("Can't bind property [%s] to field of type [%s]", property, field.getType()));
            }
        } catch (NumberFormatException e) {
            throw new RuntimeException(format("Failed to convert property [%s] value [%s] to type [%s]", property, value, field.getType()), e);
        }
    }

    private Field findField(Class clazz, String property) {
        try {
            return clazz.getDeclaredField(property);
        } catch (NoSuchFieldException e) {
            Class superClass = clazz.getSuperclass();
            if (superClass != null) return findField(clazz, property);
            else return null;
        }
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hz) {
        this.hz = hz;
    }
}

