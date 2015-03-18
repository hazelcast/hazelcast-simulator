package com.hazelcast.simulator.test;

import org.junit.Test;

import java.util.Set;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static com.hazelcast.simulator.test.Failure.Type.*;

public class TypeTest {

    @Test
    public void testFromPropertyValue_null() throws Exception {
        Set<Failure.Type> types = fromPropertyValue(null);
        assertThat(types, is(empty()));
    }

    @Test
    public void testFromPropertyValue_empty() throws Exception {
        Set<Failure.Type> types = fromPropertyValue("");
        assertThat(types, is(empty()));
    }

    @Test
    public void testFromPropertyValue_singleValue() throws Exception {
        Set<Failure.Type> types = fromPropertyValue("workerException");
        assertThat(types, hasSize(1));
        assertThat(types, contains(WORKER_EXCEPTION));
    }

    @Test
    public void testFromPropertyValue_twoValues() throws Exception {
        Set<Failure.Type> types = fromPropertyValue("workerException, workerExit");
        assertThat(types, hasSize(2));
        assertThat(types, containsInAnyOrder(WORKER_EXCEPTION, WORKER_EXIT));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromPropertyValue_unknownId() throws Exception {
        fromPropertyValue("workerException, foo");
    }

    @Test
    public void testGetIdsAsString() {
        String idsAsString = Failure.Type.getIdsAsString();
        Failure.Type[] types = Failure.Type.values();

        for (Failure.Type type : types) {
            assertTrue(idsAsString.contains(type.getId()));
        }
    }
}