package com.hazelcast.simulator.worker;

import com.hazelcast.simulator.probes.probes.Probe;
import com.hazelcast.simulator.probes.probes.Result;
import com.hazelcast.simulator.probes.probes.impl.ConcurrentProbe;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.test.annotations.Performance;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.utils.AnnotationFilter.TeardownFilter;
import com.hazelcast.simulator.utils.AnnotationFilter.VerifyFilter;
import com.hazelcast.simulator.utils.AnnotationFilter.WarmupFilter;
import com.hazelcast.simulator.utils.ThreadSpawner;
import com.hazelcast.simulator.worker.performance.PerformanceState;
import com.hazelcast.simulator.worker.performance.PerformanceTracker;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;
import com.hazelcast.simulator.worker.tasks.IWorker;
import com.hazelcast.util.Clock;
import org.apache.log4j.Logger;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getAtMostOneMethodWithoutArgs;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getAtMostOneVoidMethodSkipArgsCheck;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getAtMostOneVoidMethodWithoutArgs;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getValueFromNameAnnotation;
import static com.hazelcast.simulator.utils.PropertyBindingSupport.bindOptionalProperty;
import static com.hazelcast.simulator.utils.ReflectionUtils.getField;
import static com.hazelcast.simulator.utils.ReflectionUtils.injectObjectToInstance;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokeMethod;
import static com.hazelcast.simulator.worker.performance.PerformanceState.EMPTY_OPERATION_COUNT;
import static java.lang.String.format;

/**
 * Since the test is based on annotations there is no API we can call easily.
 * That is the task of this test container.
 *
 * @param <T> Class of type {@link com.hazelcast.simulator.test.TestContext}
 */
public class TestContainer<T extends TestContext> {

    /**
     * List of optional test properties, which are allowed to be defined in the properties file, but not in the test class.
     */
    public static final Set<String> OPTIONAL_TEST_PROPERTIES;

    private static final int DEFAULT_THREAD_COUNT = 10;

    private static final Logger LOGGER = Logger.getLogger(TestContainer.class);

    private enum OptionalTestProperties {
        THREAD_COUNT("threadCount"),
        LOG_FREQUENCY("logFrequency");

        private final String propertyName;

        OptionalTestProperties(String propertyName) {
            this.propertyName = propertyName;
        }
    }

    static {
        Set<String> optionalTestProperties = new HashSet<String>();
        for (OptionalTestProperties optionalTestProperty : OptionalTestProperties.values()) {
            optionalTestProperties.add(optionalTestProperty.propertyName);
        }
        OPTIONAL_TEST_PROPERTIES = Collections.unmodifiableSet(optionalTestProperties);
    }

    // properties
    @SuppressWarnings("checkstyle:visibilitymodifier")
    public int threadCount = DEFAULT_THREAD_COUNT;

    private final Map<String, Probe> probeMap = new ConcurrentHashMap<String, Probe>();

    private final Object testClassInstance;
    private final Class testClassType;
    private final T testContext;
    private final TestCase testCase;

    private final PerformanceTracker performanceTracker;

    private Method runMethod;
    private Method runWithWorkerMethod;

    private Method setupMethod;
    private Object[] setupArguments;

    private Method localWarmupMethod;
    private Method globalWarmupMethod;

    private Method localVerifyMethod;
    private Method globalVerifyMethod;

    private Method localTeardownMethod;
    private Method globalTeardownMethod;

    private Method operationCountMethod;

    private IWorker operationCountWorkerInstance;

    private volatile boolean isRunning = true;

    public TestContainer(Object testObject, T testContext, TestCase testCase) {
        if (testObject == null) {
            throw new NullPointerException();
        }
        if (testContext == null) {
            throw new NullPointerException();
        }

        this.testClassInstance = testObject;
        this.testClassType = testObject.getClass();
        this.testContext = testContext;
        this.testCase = testCase;
        this.performanceTracker = new PerformanceTracker();

        initMethods();
    }

    public Map<String, Result> getProbeResults() {
        Map<String, Result> results = new HashMap<String, Result>(probeMap.size());
        for (Map.Entry<String, Probe> entry : probeMap.entrySet()) {
            String probeName = entry.getKey();
            Probe probe = entry.getValue();
            if (!probe.isDisabled()) {
                Result result = probe.getResult();
                results.put(probeName, result);
            }
        }
        return results;
    }

    public T getTestContext() {
        return testContext;
    }

    public PerformanceState getPerformanceState() {
        if (!isRunning) {
            return null;
        }

        long operationCount = EMPTY_OPERATION_COUNT;
        try {
            Object testInstance = (operationCountWorkerInstance != null ? operationCountWorkerInstance : testClassInstance);
            Long count = invokeMethod(testInstance, operationCountMethod);
            if (count != null) {
                operationCount = count;
            }
        } catch (Exception e) {
            LOGGER.debug("Exception while retrieving operation count from " + testCase.getId() + ": " + e.getMessage());
        }
        return performanceTracker.update(operationCount);
    }

    public void invoke(TestPhase testPhase) throws Exception {
        switch (testPhase) {
            case SETUP:
                invokeMethod(testClassInstance, setupMethod, setupArguments);
                break;
            case LOCAL_WARMUP:
                invokeMethod(testClassInstance, localWarmupMethod);
                break;
            case GLOBAL_WARMUP:
                invokeMethod(testClassInstance, globalWarmupMethod);
                break;
            case RUN:
                run();
                break;
            case GLOBAL_VERIFY:
                invokeMethod(testClassInstance, globalVerifyMethod);
                break;
            case LOCAL_VERIFY:
                invokeMethod(testClassInstance, localVerifyMethod);
                break;
            case GLOBAL_TEARDOWN:
                invokeMethod(testClassInstance, globalTeardownMethod);
                break;
            case LOCAL_TEARDOWN:
                invokeMethod(testClassInstance, localTeardownMethod);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported test phase: " + testPhase);
        }
    }

    private void run() throws Exception {
        long now = Clock.currentTimeMillis();
        performanceTracker.start();
        for (Probe probe : probeMap.values()) {
            probe.startProbing(now);
        }
        if (runWithWorkerMethod != null) {
            invokeRunWithWorkerMethod(now);
        } else {
            invokeMethod(testClassInstance, runMethod);
        }
        isRunning = false;
        now = Clock.currentTimeMillis();
        for (Probe probe : probeMap.values()) {
            probe.stopProbing(now);
        }
    }

    private void initMethods() {
        try {
            runMethod = getAtMostOneVoidMethodWithoutArgs(testClassType, Run.class);
            runWithWorkerMethod = getAtMostOneMethodWithoutArgs(testClassType, RunWithWorker.class, IWorker.class);
            if ((runMethod == null) == (runWithWorkerMethod == null)) {
                throw new IllegalTestException(format("Test must contain either %s or %s method",
                        Run.class, RunWithWorker.class));
            }

            setupMethod = getAtMostOneVoidMethodSkipArgsCheck(testClassType, Setup.class);
            if (setupMethod != null) {
                assertSetupArguments(setupMethod);
                setupArguments = getSetupArguments(setupMethod);
            }

            localWarmupMethod = getAtMostOneVoidMethodWithoutArgs(testClassType, Warmup.class, new WarmupFilter(false));
            globalWarmupMethod = getAtMostOneVoidMethodWithoutArgs(testClassType, Warmup.class, new WarmupFilter(true));

            localVerifyMethod = getAtMostOneVoidMethodWithoutArgs(testClassType, Verify.class, new VerifyFilter(false));
            globalVerifyMethod = getAtMostOneVoidMethodWithoutArgs(testClassType, Verify.class, new VerifyFilter(true));

            localTeardownMethod = getAtMostOneVoidMethodWithoutArgs(testClassType, Teardown.class, new TeardownFilter(false));
            globalTeardownMethod = getAtMostOneVoidMethodWithoutArgs(testClassType, Teardown.class, new TeardownFilter(true));

            operationCountMethod = getAtMostOneMethodWithoutArgs(testClassType, Performance.class, Long.TYPE);
        } catch (Exception e) {
            throw new IllegalTestException(e.getMessage());
        }

        injectDependencies();
    }

    private void assertSetupArguments(Method method) {
        Class[] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length < 1) {
            return;
        }

        boolean testContextFound = false;
        boolean illegalArgumentFound = false;
        for (Class<?> parameterType : parameterTypes) {
            boolean isObject = parameterType.isAssignableFrom(Object.class);
            if (!isObject && parameterType.isAssignableFrom(TestContext.class)) {
                testContextFound = true;
            } else if (!parameterType.isAssignableFrom(Probe.class) || isObject) {
                illegalArgumentFound = true;
                break;
            }
        }
        if (!testContextFound || illegalArgumentFound) {
            throw new IllegalTestException(
                    format("Method %s.%s must have argument of type %s and zero or more arguments of type %s",
                            testClassType, method, TestContext.class, Probe.class));
        }
    }

    private Object[] getSetupArguments(Method setupMethod) {
        Class[] parameterTypes = setupMethod.getParameterTypes();
        Object[] arguments = new Object[parameterTypes.length];
        for (int i = 0; i < parameterTypes.length; i++) {
            arguments[i] = getSetupArgumentForParameterType(parameterTypes[i], i);
        }
        return arguments;
    }

    private Object getSetupArgumentForParameterType(Class<?> parameterType, int index) {
        if (parameterType.isAssignableFrom(TestContext.class)) {
            return testContext;
        }
        throw new IllegalTestException(format("Unknown parameter type %s at index %s in setup method", parameterType, index));
    }

    private void injectDependencies() {
        Field[] fields = testClassType.getDeclaredFields();
        for (Field field : fields) {
            String probeName = getValueFromNameAnnotation(field);
            if (Probe.class.equals(field.getType())) {
                Probe probe = getOrCreateConcurrentProbe(probeName);
                injectObjectToInstance(testClassInstance, field, probe);
            }
        }
    }

    private Probe getOrCreateConcurrentProbe(String probeName) {
        Probe probe = probeMap.get(probeName);
        if (probe == null) {
            probe = new ConcurrentProbe();
            probeMap.put(probeName, probe);
        }
        return probe;
    }

    private void invokeRunWithWorkerMethod(long now) throws Exception {
        bindOptionalProperty(this, testCase, OptionalTestProperties.THREAD_COUNT.propertyName);

        String testId = (testContext.getTestId().isEmpty() ? "Default" : testContext.getTestId());

        LOGGER.info(format("Spawning %d worker threads for test %s", threadCount, testId));
        if (threadCount <= 0) {
            return;
        }

        // create instance to get class of worker
        Class workerClass = invokeMethod(testClassInstance, runWithWorkerMethod).getClass();

        Field testContextField = getField(workerClass, "testContext", TestContext.class);
        Field workerProbeField = getField(workerClass, "workerProbe", Probe.class);

        Method optionalMethod = getAtMostOneMethodWithoutArgs(workerClass, Performance.class, Long.TYPE);
        if (optionalMethod != null) {
            operationCountMethod = optionalMethod;
        }

        // create one concurrent probe per test and inject it in all worker instances of the test
        Probe probe = getOrCreateConcurrentProbe(testId + "WorkerProbe");
        probe.startProbing(now);

        operationCountMethod = getAtMostOneMethodWithoutArgs(AbstractWorker.class, Performance.class, Long.TYPE);

        // spawn worker and wait for completion
        spawnWorkerThreads(testContextField, workerProbeField, probe);

        // call the afterCompletion method on a single instance of the worker
        operationCountWorkerInstance.afterCompletion();
    }

    private void spawnWorkerThreads(Field testContextField, Field workerProbeField, Probe probe) throws Exception {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int i = 0; i < threadCount; i++) {
            IWorker worker = invokeMethod(testClassInstance, runWithWorkerMethod);

            if (testContextField != null) {
                injectObjectToInstance(worker, testContextField, testContext);
            }
            if (workerProbeField != null) {
                injectObjectToInstance(worker, workerProbeField, probe);
            }

            bindOptionalProperty(worker, testCase, OptionalTestProperties.LOG_FREQUENCY.propertyName);

            operationCountWorkerInstance = worker;

            spawner.spawn(worker);
        }
        spawner.awaitCompletion();
    }

    boolean hasProbe(String probeName) {
        return probeMap.keySet().contains(probeName);
    }
}
