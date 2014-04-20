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
package com.hazelcast.stabilizer.tests;

import com.hazelcast.stabilizer.TestRecipe;
import com.hazelcast.stabilizer.agent.WorkerJvmSettings;

import javax.xml.bind.annotation.XmlRootElement;
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

@XmlRootElement
public class Workout implements Serializable {

    private static final long serialVersionUID = 1;

    public static Workout createWorkout(File file) throws Exception {
        Properties properties = loadProperties(file);

        Map<String, TestRecipe> recipies = new HashMap<String, TestRecipe>();
        for (String property : properties.stringPropertyNames()) {
            String value = (String) properties.get(property);
            int indexOfDot = property.indexOf(".");

            String recipeId = "";
            String field = property;
            if (indexOfDot > -1) {
                recipeId = property.substring(0, indexOfDot);
                field = property.substring(indexOfDot + 1);
            }

            TestRecipe recipe = recipies.get(recipeId);
            if (recipe == null) {
                recipe = new TestRecipe();
                recipies.put(recipeId, recipe);
            }

            recipe.setProperty(field, value);
        }

        List<String> recipeIds = new LinkedList<String>(recipies.keySet());
        Collections.sort(recipeIds);

        Workout workout = new Workout();
        for (String recipeId : recipeIds) {
            TestRecipe recipe = recipies.get(recipeId);
            if (recipe.getClassname() == null) {
                if ("".equals(recipeId)) {
                    throw new RuntimeException(format("There is no class set for the in property file [%s]." +
                                    "Add class=YourTestClass",
                            file.getAbsolutePath()
                    ));
                } else {
                    throw new RuntimeException(format("There is no class set for test [%s] in property file [%s]." +
                                    "Add %s.class=YourTestClass",
                            recipeId, file.getAbsolutePath(), recipeId
                    ));
                }
            }
            workout.addTest(recipe);
        }
        return workout;
    }

    public final String id = "" + System.currentTimeMillis();
    public List<TestRecipe> testRecipeList = new LinkedList<TestRecipe>();
    public int duration;
    public WorkerJvmSettings workerJvmSettings;
    public boolean failFast;

    public void addTest(TestRecipe testRecipe) {
        testRecipeList.add(testRecipe);
    }

    public int size() {
        return testRecipeList.size();
    }

    @Override
    public String toString() {
        return "Workout{" +
                "duration=" + duration +
                ", id='" + id + '\'' +
                ", testRecipeList=" + testRecipeList +
                ", workerSettings=" + workerJvmSettings +
                ", failFast=" + failFast +
                '}';
    }
}
