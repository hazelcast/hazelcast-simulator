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
package com.hazelcast.simulator.test;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TestCase implements Serializable {
    public String id;
    public HashMap<String, String> properties = new HashMap<String, String>();

    public String getClassname() {
        return properties.get("class");
    }

    public String getId() {
        return id;
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

    public void override(Map<String, String> propertiesOverride) {
        for (String key : properties.keySet()) {
            if (propertiesOverride.containsKey(key)) {
                properties.put(key, propertiesOverride.get(key));
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("TestCase{");
        sb.append("\n      ").append("id=").append(id);
        sb.append("\n    , ").append("class=").append(getClassname());

        List<String> keys = new LinkedList<String>(properties.keySet());
        Collections.sort(keys);

        for (String key : keys) {
            if (!"class".equals(key)) {
                sb.append("\n    , ").append(key).append("=").append(properties.get(key));
            }
        }
        sb.append("\n}");
        return sb.toString();
    }
}
