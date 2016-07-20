package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.utils.AnnotationFilter.TeardownFilter;
import com.hazelcast.simulator.utils.AnnotationFilter.VerifyFilter;
import com.hazelcast.simulator.utils.AnnotationFilter.WarmupFilter;
import org.junit.Test;

import java.lang.reflect.Method;

import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.ALWAYS_FILTER;
import static org.junit.Assert.assertEquals;

public class AnnotationFilterTest {

    @Test
    public void testAlwaysFilter() {
        Method method = new AnnotatedMethodRetriever(AnnotationTestClass.class, Setup.class)
                .withVoidReturnType()
                .withVoidReturnType()
                .withFilter(ALWAYS_FILTER)
                .find();
        assertEquals("setupMethod", method.getName());
    }

    @Test
    public void testLocalTeardownFilter() {
        Method method = new AnnotatedMethodRetriever(AnnotationTestClass.class, Teardown.class)
                .withVoidReturnType()
                .withVoidReturnType()
                .withFilter(new TeardownFilter(false))
                .find();

        assertEquals("localTearDown", method.getName());
    }

    @Test
    public void testGlobalTeardownFilter() {
        Method method = new AnnotatedMethodRetriever(AnnotationTestClass.class, Teardown.class)
                .withVoidReturnType()
                .withVoidReturnType()
                .withFilter(new TeardownFilter(true))
                .find();
        assertEquals("globalTearDown", method.getName());
    }

    @Test
    public void testLocalWarmupFilter() {
        Method method = new AnnotatedMethodRetriever(AnnotationTestClass.class, Warmup.class)
                .withVoidReturnType()
                .withVoidReturnType()
                .withFilter(new WarmupFilter(false))
                .find();

        assertEquals("localWarmup", method.getName());
    }

    @Test
    public void testGlobalWarmupFilter() {
        Method method = new AnnotatedMethodRetriever(AnnotationTestClass.class, Warmup.class)
                .withVoidReturnType()
                .withVoidReturnType()
                .withFilter(new WarmupFilter(true))
                .find();
        assertEquals("globalWarmup", method.getName());
    }

    @Test
    public void testLocalVerifyFilter() {
        Method method = new AnnotatedMethodRetriever(AnnotationTestClass.class, Verify.class)
                .withVoidReturnType()
                .withVoidReturnType()
                .withFilter(new VerifyFilter(false))
                .find();
        assertEquals("localVerify", method.getName());
    }

    @Test
    public void testGlobalVerifyFilter() {
        Method method = new AnnotatedMethodRetriever(AnnotationTestClass.class, Verify.class)
                .withVoidReturnType()
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

        @Warmup(global = false)
        public void localWarmup() {
        }

        @Warmup(global = true)
        public void globalWarmup() {
        }

        @Verify(global = false)
        public void localVerify() {
        }

        @Verify(global = true)
        public void globalVerify() {
        }
    }
}
