package com.hazelcast.simulator.tests.special;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.probes.probes.IntervalProbe;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Performance;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.utils.ExceptionReporter;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static com.hazelcast.simulator.utils.ReflectionUtils.getField;
import static com.hazelcast.simulator.utils.ReflectionUtils.getMethodByName;
import static com.hazelcast.simulator.utils.ReflectionUtils.getObjectFromField;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ReflectionPerformanceTest {

    private static final ILogger LOGGER = Logger.getLogger(ReflectionPerformanceTest.class);

    // properties
    public long resultLimit = Integer.MAX_VALUE;
    public IntervalProbe instanceProbe;
    public IntervalProbe reflectionProbe;

    private TestContext testContext;

    private NotAvailableClass realInstance;

    private Object reflectionInstance;
    private Method reflectionMethod;
    private Field reflectionField;

    private volatile long expectedCounter;

    @Setup
    public void setUp(TestContext testContext) {
        this.testContext = testContext;

        realInstance = new NotAvailableClass();

        try {
            Class classType = Class.forName("com.hazelcast.simulator.tests.special.ReflectionPerformanceTest$NotAvailableClass");
            reflectionInstance = classType.newInstance();
            reflectionMethod = getMethodByName(classType, "shouldBeCalled");
            reflectionField = getField(classType, "invokeCounter", Long.TYPE);
        } catch (Exception e) {
            LOGGER.warning("Reflection error in setUp()", e);
        }
        if (realInstance == null || reflectionMethod == null || reflectionField == null) {
            throw new RuntimeException("NotAvailableClass could not be found via reflection!");
        }
    }

    @Verify
    public void verify() {
        long expectedCounter = this.expectedCounter;
        long realInstanceCounter = realInstance.invokeCounter;
        Long reflectionCounter = getObjectFromField(reflectionInstance, "invokeCounter");
        assertNotNull("Invocations on reflection instance should not be null", reflectionCounter);

        LOGGER.info("expectedCounter: " + expectedCounter);
        LOGGER.info("realInstanceCounter: " + realInstanceCounter);
        LOGGER.info("reflectionCounter: " + reflectionCounter);
        LOGGER.info("instanceProbe: " + instanceProbe.getInvocationCount());
        LOGGER.info("reflectionProbe: " + reflectionProbe.getInvocationCount());

        assertEquals("Invocations on real instance should match", expectedCounter, realInstanceCounter);
        assertEquals("Invocations on reflection instance should match", expectedCounter, reflectionCounter.intValue());
    }

    @Performance
    public long performance() {
        return expectedCounter;
    }

    @Run
    public void run() {
        while (!testContext.isStopped()) {
            instanceProbe.started();
            realInstance.shouldBeCalled();
            instanceProbe.done();

            try {
                reflectionProbe.started();
                reflectionMethod.invoke(reflectionInstance);
                reflectionProbe.done();
            } catch (Exception e) {
                ExceptionReporter.report(testContext.getTestId(), e);
            }

            long expectedCounter = this.expectedCounter + 1;
            this.expectedCounter = expectedCounter;
            if (expectedCounter == resultLimit) {
                break;
            }
        }
    }

    public static class NotAvailableClass {

        private volatile long invokeCounter;

        public void shouldBeCalled() {
            invokeCounter++;
        }
    }

    @SuppressWarnings("unused")
    public interface INotAvailableClass {

        void shouldBeCalled();
    }
}
