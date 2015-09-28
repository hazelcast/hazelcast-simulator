package com.hazelcast.simulator.tests.special;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.probes.probes.Probe;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestException;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.Performance;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.special.helpers.InvocationTestClass;
import com.hazelcast.simulator.tests.special.helpers.InvocationTestInterface;
import com.hazelcast.simulator.utils.ExceptionReporter;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.utils.ReflectionUtils.getMethodByName;
import static com.hazelcast.simulator.utils.ReflectionUtils.getObjectFromField;
import static com.hazelcast.simulator.utils.compiler.InMemoryJavaCompiler.compile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * This test measures the latency of method invocations on a normal class instance versus a runtime compiled interface
 * implementation and invocations via reflection.
 *
 * The test is not meant to be executed as normal test on a Hazelcast cluster, but to help to decide in which way we may implement
 * tests for Hazelcast classes which are not available at compile time, e.g. to keep compatible with deprecated API of older
 * Hazelcast versions.
 */
public class InvocationLatencyTest {

    private static final ILogger LOGGER = Logger.getLogger(InvocationLatencyTest.class);

    // properties
    public long resultLimit = Integer.MAX_VALUE;

    // probes
    public Probe classInstanceProbe;
    public Probe interfaceInstanceProbe;
    public Probe reflectionInstanceProbe;

    private final AtomicLong expectedCounter = new AtomicLong(0);

    private TestContext testContext;
    private InvocationTestClass classInstance;
    private InvocationTestInterface interfaceInstance;
    private Object reflectionInstance;
    private Method reflectionMethod;

    @Setup
    public void setUp(TestContext testContext) {
        this.testContext = testContext;

        try {
            classInstance = new InvocationTestClass();

            interfaceInstance = getInterfaceInstance();
            if (interfaceInstance == null) {
                throw new TestException("InvocationTestClassRuntime could not be compiled!");
            }

            Class classType = Class.forName("com.hazelcast.simulator.tests.special.helpers.InvocationTestClass");
            reflectionInstance = classType.newInstance();
            reflectionMethod = getMethodByName(classType, "shouldBeCalled");
            if (reflectionInstance == null || reflectionMethod == null) {
                throw new TestException("InvocationTestClass could not be found via reflection!");
            }
        } catch (Exception e) {
            ExceptionReporter.report(testContext.getTestId(), e);
            throw new TestException(e);
        }
    }

    private InvocationTestInterface getInterfaceInstance() throws Exception {
        String source = InvocationTestClass.getSource();
        source = source.replaceFirst("InvocationTestClass", "InvocationTestClassRuntime implements InvocationTestInterface");
        Class<?> interfaceClass = compile("com.hazelcast.simulator.tests.special.helpers.InvocationTestClassRuntime", source);

        return (InvocationTestInterface) interfaceClass.newInstance();
    }

    @Verify
    public void verify() {
        long expectedCounter = this.expectedCounter.get();
        LOGGER.info("expectedCounter: " + expectedCounter);

        long classInstanceCounter = classInstance.getInvokeCounter();
        long interfaceInstanceCounter = interfaceInstance.getInvokeCounter();
        Long reflectionCounter = getObjectFromField(reflectionInstance, "invokeCounter");
        assertNotNull("Invocations on reflection instance should not be null", reflectionCounter);

        LOGGER.info("classInstanceCounter: " + classInstanceCounter);
        LOGGER.info("interfaceInstanceCounter: " + interfaceInstanceCounter);
        LOGGER.info("reflectionCounter: " + reflectionCounter);

        assertEquals("Invocations on class instance should match", expectedCounter, classInstanceCounter);
        assertEquals("Invocations on interface instance should match", expectedCounter, interfaceInstanceCounter);
        assertEquals("Invocations on reflection instance should match", expectedCounter, reflectionCounter.intValue());
    }

    @Performance
    public long performance() {
        return expectedCounter.get();
    }

    @Run
    public void run() {
        while (!testContext.isStopped()) {
            classInstanceProbe.started();
            classInstance.shouldBeCalled();
            classInstanceProbe.done();

            interfaceInstanceProbe.started();
            interfaceInstance.shouldBeCalled();
            interfaceInstanceProbe.done();

            try {
                reflectionInstanceProbe.started();
                reflectionMethod.invoke(reflectionInstance);
                reflectionInstanceProbe.done();
            } catch (Exception e) {
                ExceptionReporter.report(testContext.getTestId(), e);
            }

            if (expectedCounter.incrementAndGet() == resultLimit) {
                break;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        InvocationLatencyTest test = new InvocationLatencyTest();
        new TestRunner<InvocationLatencyTest>(test).withDuration(5).run();
    }
}
