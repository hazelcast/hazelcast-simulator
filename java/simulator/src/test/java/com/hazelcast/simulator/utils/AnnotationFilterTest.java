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
