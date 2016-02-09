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
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getAtMostOneVoidMethodWithoutArgs;
import static org.junit.Assert.assertEquals;

public class AnnotationFilterTest {

    @Test
    public void testAlwaysFilter() {
        Method method = getAtMostOneVoidMethodWithoutArgs(AnnotationTestClass.class, Setup.class, ALWAYS_FILTER);
        assertEquals("setupMethod", method.getName());
    }

    @Test
    public void testLocalTeardownFilter() {
        Method method = getAtMostOneVoidMethodWithoutArgs(AnnotationTestClass.class, Teardown.class, new TeardownFilter(false));
        assertEquals("localTearDown", method.getName());
    }

    @Test
    public void testGlobalTeardownFilter() {
        Method method = getAtMostOneVoidMethodWithoutArgs(AnnotationTestClass.class, Teardown.class, new TeardownFilter(true));
        assertEquals("globalTearDown", method.getName());
    }

    @Test
    public void testLocalWarmupFilter() {
        Method method = getAtMostOneVoidMethodWithoutArgs(AnnotationTestClass.class, Warmup.class, new WarmupFilter(false));
        assertEquals("localWarmup", method.getName());
    }

    @Test
    public void testGlobalWarmupFilter() {
        Method method = getAtMostOneVoidMethodWithoutArgs(AnnotationTestClass.class, Warmup.class, new WarmupFilter(true));
        assertEquals("globalWarmup", method.getName());
    }

    @Test
    public void testLocalVerifyFilter() {
        Method method = getAtMostOneVoidMethodWithoutArgs(AnnotationTestClass.class, Verify.class, new VerifyFilter(false));
        assertEquals("localVerify", method.getName());
    }

    @Test
    public void testGlobalVerifyFilter() {
        Method method = getAtMostOneVoidMethodWithoutArgs(AnnotationTestClass.class, Verify.class, new VerifyFilter(true));
        assertEquals("globalVerify", method.getName());
    }

    @SuppressWarnings("DefaultAnnotationParam")
    private static class AnnotationTestClass {

        @Setup
        private void setupMethod() {
        }

        @Teardown(global = false)
        private void localTearDown() {
        }

        @Teardown(global = true)
        private void globalTearDown() {
        }

        @Warmup(global = false)
        private void localWarmup() {
        }

        @Warmup(global = true)
        private void globalWarmup() {
        }

        @Verify(global = false)
        private void localVerify() {
        }

        @Verify(global = true)
        private void globalVerify() {
        }
    }
}
