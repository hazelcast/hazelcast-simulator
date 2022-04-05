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
