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

import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.hazelcast.simulator.TestSupport.toMap;
import static org.junit.Assert.assertEquals;

public class TagUtilsTest {

    @Test
    public void parseTags() {
        Map<String, String> result = TagUtils.parseTags("a,b=10,c=20");

        assertEquals(toMap("a", "", "b", "10", "c", "20"), result);
    }

    @Test
    public void parseTags_whenEmpty() {
        Map<String, String> result = TagUtils.parseTags("");

        assertEquals(toMap(), result);
    }

    @Test
    public void tagsToString() {
        Map<String, String> hashMap = new LinkedHashMap<String, String>();
        hashMap.put("a", "");
        hashMap.put("b", "10");
        hashMap.put("c", "20");

        String result = TagUtils.tagsToString(hashMap);

        assertEquals("a,b=10,c=20", result);
    }

    @Test
    public void match() {
        assertMatches(true, "", "");
        assertMatches(true, "", "a");
        assertMatches(true, "", "a=10");
        assertMatches(true, "a=10", "a=10");
        assertMatches(true, "a=10", "a=10,b=20");
        assertMatches(true, "a", "a");

        assertMatches(false, "a", "");
        assertMatches(false, "a", "b");
        assertMatches(false, "a=10", "a=20");
        assertMatches(false, "a=10,b=10", "a=10,b=20");
    }

    private void assertMatches(boolean match, String required, String actual) {
        Map<String, String> e = TagUtils.parseTags(required);
        Map<String, String> a = TagUtils.parseTags(actual);
        assertEquals(match, TagUtils.matches(e, a));
    }
}
