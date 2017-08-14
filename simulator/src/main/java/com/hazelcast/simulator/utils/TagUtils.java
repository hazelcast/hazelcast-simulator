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

package com.hazelcast.simulator.utils;

import java.util.HashMap;
import java.util.Map;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public final class TagUtils {

    private TagUtils() {
    }

    public static boolean matches(Map<String, String> expectedTags, Map<String, String> actualTags) {
        for (Map.Entry<String, String> entry : expectedTags.entrySet()) {
            String key = entry.getKey();
            String expected = expectedTags.get(key);
            String actual = actualTags.get(key);
            if (!expected.equals(actual)) {
                return false;
            }
        }

        return true;
    }

    public static boolean anyMatches(Map<String, String> expectedTags, Map<String, String> actualTags) {
        for (Map.Entry<String, String> entry : expectedTags.entrySet()) {
            String key = entry.getKey();
            String expected = expectedTags.get(key);
            String actual = actualTags.get(key);
            if (expected.equals(actual)) {
                return true;
            }
        }

        return false;
    }


    public static Map<String, String> parseTags(String s) {
        Map<String, String> result = new HashMap<String, String>();
        if ("".equals(s)) {
            return result;
        }
        for (String keyValue : s.split(",")) {
            if (keyValue.contains("=")) {
                String[] array = keyValue.split("=");
                result.put(array[0], array[1]);
            } else {
                result.put(keyValue, "");
            }
        }

        return result;
    }

    public static String tagsToString(Map<String, String> map) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (first) {
                first = false;
            } else {
                sb.append(',');
            }

            String key = entry.getKey();
            String value = entry.getValue();
            if (value.equals("")) {
                sb.append(key);
            } else {
                sb.append(key).append('=').append(value);
            }
        }
        return sb.toString();
    }

    public static Map<String, String> loadTags(OptionSet options, OptionSpec<String> spec) {
        if (!options.has(spec)) {
            return new HashMap<String, String>();
        }

        return parseTags(options.valueOf(spec));
    }
}
