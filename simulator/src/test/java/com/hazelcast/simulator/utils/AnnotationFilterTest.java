/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.utils.AnnotationFilter.PrepareFilter;
import com.hazelcast.simulator.utils.AnnotationFilter.TeardownFilter;
import com.hazelcast.simulator.utils.AnnotationFilter.VerifyFilter;
import org.junit.Test;

import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;

public class AnnotationFilterTest {

    @Test
    public void testLocalTeardownFilter() {
        Method method = new AnnotatedMethodRetriever(AnnotationTestClass.class, Teardown.class)
                .withVoidReturnType()
                .withFilter(new TeardownFilter(false))
                .find();

        assertEquals("localTearDown", method.getName());
    }

    @Test
    public void testGlobalTeardownFilter() {
        Method method = new AnnotatedMethodRetriever(AnnotationTestClass.class, Teardown.class)
                .withVoidReturnType()
                .withFilter(new TeardownFilter(true))
                .find();
        assertEquals("globalTearDown", method.getName());
    }

    @Test
    public void testLocalWarmupFilter() {
        Method method = new AnnotatedMethodRetriever(AnnotationTestClass.class, Prepare.class)
                .withVoidReturnType()
                .withFilter(new PrepareFilter(false))
                .find();

        assertEquals("localPrepare", method.getName());
    }

    @Test
    public void testGlobalWarmupFilter() {
        Method method = new AnnotatedMethodRetriever(AnnotationTestClass.class, Prepare.class)
                .withVoidReturnType()
                .withFilter(new PrepareFilter(true))
                .find();
        assertEquals("globalPrepare", method.getName());
    }

    @Test
    public void testLocalVerifyFilter() {
        Method method = new AnnotatedMethodRetriever(AnnotationTestClass.class, Verify.class)
                .withVoidReturnType()
                .withFilter(new VerifyFilter(false))
                .find();
        assertEquals("localVerify", method.getName());
    }

    @Test
    public void testGlobalVerifyFilter() {
        Method method = new AnnotatedMethodRetriever(AnnotationTestClass.class, Verify.class)
                .withVoidReturnType()
                .withFilter(new VerifyFilter(true))
                .find();

        assertEquals("globalVerify", method.getName());
    }

    @SuppressWarnings("DefaultAnnotationParam")
    private static class AnnotationTestClass {

        @Setup
        public void setupMethod() {
        }

        @Teardown(global = false)
        public void localTearDown() {
        }

        @Teardown(global = true)
        public void globalTearDown() {
        }

        @Prepare(global = false)
        public void localPrepare() {
        }

        @Prepare(global = true)
        public void globalPrepare() {
        }

        @Verify(global = false)
        public void localVerify() {
        }

        @Verify(global = true)
        public void globalVerify() {
        }
    }
}
