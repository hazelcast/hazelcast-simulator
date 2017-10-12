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
    public void testFromPropertyValue_null() {
        Set<FailureType> types = fromPropertyValue(null);
        assertThat(types, is(empty()));
    }

    @Test
    public void testFromPropertyValue_empty() {
        Set<FailureType> types = fromPropertyValue("");
        assertThat(types, is(empty()));
    }

    @Test
    public void testFromPropertyValue_singleValue() {
        Set<FailureType> types = fromPropertyValue("workerException");
        assertThat(types, hasSize(1));
        assertThat(types, contains(WORKER_EXCEPTION));
    }

    @Test
    public void testFromPropertyValue_twoValues() {
        Set<FailureType> types = fromPropertyValue("workerException, workerAbnormalExit");
        assertThat(types, hasSize(2));
        assertThat(types, containsInAnyOrder(WORKER_EXCEPTION, WORKER_ABNORMAL_EXIT));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromPropertyValue_unknownId() {
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