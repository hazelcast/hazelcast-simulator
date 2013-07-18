package com.hazelcast.heartattack;

import java.io.Serializable;
import java.util.*;

public class ExerciseRecipe implements Serializable {
    private static final long serialVersionUID = 1;
    private final String exerciseId = UUID.randomUUID().toString();

    private Map<String, String> properties = new HashMap<String,String>();

    public String getClassname() {
        return properties.get("class");
    }

    public String getExerciseId() {
        return exerciseId;
    }

    public String getProperty(String name) {
        return properties.get(name);
    }

    public void setProperty(String name, String value) {
        properties.put(name, value);
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ExerciseRecipe{");
        sb.append("\n      ").append("exerciseId=").append(exerciseId);
        sb.append("\n    , ").append("class=").append(getClassname());

        List<String> keys = new LinkedList<String>(properties.keySet());
        Collections.sort(keys);

        for(String key : keys){
            if(!"class".equals(key)){
                sb.append("\n    , ").append(key).append("=").append(properties.get(key));
            }
        }
        sb.append("\n}");
        return sb.toString();
    }
}
