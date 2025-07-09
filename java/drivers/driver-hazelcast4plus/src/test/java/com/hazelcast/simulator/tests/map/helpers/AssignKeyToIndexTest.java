package com.hazelcast.simulator.tests.map.helpers;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.assignKeyToIndex;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class AssignKeyToIndexTest {

    @Test
    public void testSequentialAssignment() {
        int indexUpperBound = 5;
        Map<String, Integer> indexMap = new HashMap<>();
        assertThat(assignKeyToIndex(indexUpperBound, "a", indexMap), equalTo(0));
        assertThat(assignKeyToIndex(indexUpperBound, "b", indexMap), equalTo(1));
        assertThat(assignKeyToIndex(indexUpperBound, "c", indexMap), equalTo(2));
        assertThat(assignKeyToIndex(indexUpperBound, "d", indexMap), equalTo(3));
        assertThat(assignKeyToIndex(indexUpperBound, "e", indexMap), equalTo(4));
        assertThat(assignKeyToIndex(indexUpperBound, "f", indexMap), equalTo(0));

        assertThat(indexMap, equalTo(Map.of("a", 0, "b", 1, "c", 2, "d", 3, "e", 4, "f", 0)));
    }

    @Test
    public void testAssignmentToUnbalancedMap() {
        int indexUpperBound = 3;
        Map<String, Integer> initialMap = Map.of("a", 2, "b", 2, "c", 2, "d", 0, "e", 0, "f", 0);
        Map<String, Integer> underTest = new HashMap<>(initialMap);
        assertThat(assignKeyToIndex(indexUpperBound, "g", underTest), equalTo(1));
        Map<String, Integer> expected = new HashMap<>(initialMap);
        expected.put("g", 1);
        assertThat(underTest, equalTo(expected));
    }

    @Test
    public void testDuplicateAssignment() {
        int indexUpperBound = 3;
        Map<String, Integer> initialMap = Map.of("a", 1, "b", 2, "c", 0);
        Map<String, Integer> underTest = new HashMap<>(initialMap);
        assertThat(assignKeyToIndex(indexUpperBound, "a", underTest), equalTo(1));
        assertThat(underTest, equalTo(initialMap));
    }
}
