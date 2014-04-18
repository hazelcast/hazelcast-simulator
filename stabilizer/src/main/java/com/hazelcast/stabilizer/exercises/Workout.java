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
package com.hazelcast.stabilizer.exercises;

import com.hazelcast.stabilizer.ExerciseRecipe;
import com.hazelcast.stabilizer.trainee.TraineeVmSettings;

import java.io.File;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.hazelcast.stabilizer.Utils.loadProperties;
import static java.lang.String.format;

public class Workout implements Serializable {

    private static final long serialVersionUID = 1;

    public static Workout createWorkout(File file) throws Exception {
        Properties properties = loadProperties(file);

        Map<String, ExerciseRecipe> recipies = new HashMap<String, ExerciseRecipe>();
        for (String property : properties.stringPropertyNames()) {
            String value = (String) properties.get(property);
            int indexOfDot = property.indexOf(".");

            String recipeId = "";
            String field = property;
            if (indexOfDot > -1) {
                recipeId = property.substring(0, indexOfDot);
                field = property.substring(indexOfDot + 1);
            }

            ExerciseRecipe recipe = recipies.get(recipeId);
            if (recipe == null) {
                recipe = new ExerciseRecipe();
                recipies.put(recipeId, recipe);
            }

            recipe.setProperty(field, value);
        }

        List<String> recipeIds = new LinkedList<String>(recipies.keySet());
        Collections.sort(recipeIds);

        Workout workout = new Workout();
        for (String recipeId : recipeIds) {
            ExerciseRecipe recipe = recipies.get(recipeId);
            if (recipe.getClassname() == null) {
                if ("".equals(recipeId)) {
                    throw new RuntimeException(format("There is no class set for the in property file [%s]." +
                                    "Add class=YourExerciseClass",
                            file.getAbsolutePath()
                    ));
                } else {
                    throw new RuntimeException(format("There is no class set for exercise [%s] in property file [%s]." +
                                    "Add %s.class=YourExerciseClass",
                            recipeId, file.getAbsolutePath(), recipeId
                    ));
                }
            }
            workout.addExercise(recipe);
        }
        return workout;
    }

    private final String id = "" + System.currentTimeMillis();

    private List<ExerciseRecipe> exerciseRecipeList = new LinkedList<ExerciseRecipe>();
    private int duration;
    private TraineeVmSettings traineeVmSettings;
    private boolean failFast;

    public String getId() {
        return id;
    }

    public boolean isFailFast() {
        return failFast;
    }

    public void setFailFast(boolean failFast) {
        this.failFast = failFast;
    }

    public TraineeVmSettings getTraineeVmSettings() {
        return traineeVmSettings;
    }

    public void setTraineeVmSettings(TraineeVmSettings traineeVmSettings) {
        this.traineeVmSettings = traineeVmSettings;
    }

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }

    public void addExercise(ExerciseRecipe exerciseRecipe) {
        exerciseRecipeList.add(exerciseRecipe);
    }

    public List<ExerciseRecipe> getExerciseRecipeList() {
        return exerciseRecipeList;
    }

    public int size() {
        return exerciseRecipeList.size();
    }

    @Override
    public String toString() {
        return "Workout{" +
                "duration=" + duration +
                ", id='" + id + '\'' +
                ", exerciseList=" + exerciseRecipeList +
                ", traineeSettings=" + traineeVmSettings +
                ", failFast=" + failFast +
                '}';
    }
}
