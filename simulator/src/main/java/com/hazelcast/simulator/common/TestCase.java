/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.common;

import com.hazelcast.simulator.utils.PropertyBindingSupport;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;

public class TestCase {
    private static final Pattern TEST_ID_PATTERN = Pattern.compile("^[a-zA-Z0-9-]+$");

    private static final Logger LOGGER = Logger.getLogger(PropertyBindingSupport.class);

    private String id;
    private final Map<String, String> properties = new HashMap<String, String>();

    public TestCase(String id) {
        this(id, Collections.EMPTY_MAP);
    }

    public TestCase(String id, Map<String, String> properties) {
        this.id = checkId(id);
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            setProperty(entry.getKey(), entry.getValue());
        }
    }

    public static String checkId(String id) {
        if (id == null) {
            throw new NullPointerException("test id can't be null");
        }

        if ("".equals(id) || TEST_ID_PATTERN.matcher(id).matches()) {
            return id;
        }

        throw new IllegalArgumentException("test id [" + id + "] is not a valid identifier");
    }

    public void setId(String id) {
        this.id = checkId(id);
    }

    public String getId() {
        return id;
    }

    public String getClassname() {
        return properties.get("class");
    }

    public String getProperty(String name) {
        return properties.get(name);
    }

    public TestCase setProperty(String name, String value) {
        if ("basename".equals(name)) {
            LOGGER.warn("Property 'basename' is deprecated, use 'name' instead. Property has been automatically upgraded.");
            name = "name";
        }
        properties.put(name, value.trim());
        return this;
    }

    public TestCase setProperty(String name, Object v) {
        String value = v instanceof Class ? ((Class) v).getName() : v.toString();
        return setProperty(name, value);
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void override(Map<String, String> propertiesOverride) {
        for (String key : properties.keySet()) {
            if (propertiesOverride.containsKey(key)) {
                setProperty(key, propertiesOverride.get(key));
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("TestCase{");
        sb.append(NEW_LINE).append("    ").append("id=").append(id);
        sb.append(',').append(NEW_LINE).append("    ").append("class=").append(getClassname());

        List<String> keys = new LinkedList<String>(properties.keySet());
        Collections.sort(keys);

        for (String key : keys) {
            if (!"class".equals(key)) {
                sb.append(',').append(NEW_LINE).append("    ").append(key).append('=').append(properties.get(key));
            }
        }
        sb.append(NEW_LINE).append('}');
        return sb.toString();
    }
}
