package com.hazelcast.simulator.common;

import org.junit.Test;

import java.util.Set;

import static com.hazelcast.simulator.common.FailureType.WORKER_ABNORMAL_EXIT;
import static com.hazelcast.simulator.common.FailureType.WORKER_EXCEPTION;
import static com.hazelcast.simulator.common.FailureType.fromPropertyValue;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class FailureTypeTest {

    @Test
    public void testFromPropertyValue_null() throws Exception {
        Set<FailureType> types = fromPropertyValue(null);
        assertThat(types, is(empty()));
    }

    @Test
    public void testFromPropertyValue_empty() throws Exception {
        Set<FailureType> types = fromPropertyValue("");
        assertThat(types, is(empty()));
    }

    @Test
    public void testFromPropertyValue_singleValue() throws Exception {
        Set<FailureType> types = fromPropertyValue("workerException");
        assertThat(types, hasSize(1));
        assertThat(types, contains(WORKER_EXCEPTION));
    }

    @Test
    public void testFromPropertyValue_twoValues() throws Exception {
        Set<FailureType> types = fromPropertyValue("workerException, workerAbnormalExit");
        assertThat(types, hasSize(2));
        assertThat(types, containsInAnyOrder(WORKER_EXCEPTION, WORKER_ABNORMAL_EXIT));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromPropertyValue_unknownId() throws Exception {
        fromPropertyValue("workerException, foo");
    }

    @Test
    public void testGetIdsAsString() {
        String idsAsString = FailureType.getIdsAsString();
        FailureType[] types = FailureType.values();

        for (FailureType type : types) {
            assertTrue(idsAsString.contains(type.getId()));
        }
    }

    @Test
    public void testToString() {
        FailureType[] types = FailureType.values();

        for (FailureType type : types) {
            assertNotNull(type.toString());
        }
    }
}