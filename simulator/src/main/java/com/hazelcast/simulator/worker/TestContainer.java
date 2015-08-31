package com.hazelcast.simulator.worker;

import com.hazelcast.simulator.common.messaging.Message;
import com.hazelcast.simulator.probes.probes.IntervalProbe;
import com.hazelcast.simulator.probes.probes.Probes;
import com.hazelcast.simulator.probes.probes.ProbesConfiguration;
import com.hazelcast.simulator.probes.probes.ProbesType;
import com.hazelcast.simulator.probes.probes.Result;
import com.hazelcast.simulator.probes.probes.SimpleProbe;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.test.annotations.Performance;
import com.hazelcast.simulator.test.annotations.Receive;
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
import com.hazelcast.simulator.worker.commands.PerformanceState;
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
import java.util.List;
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
        WORKER_PROBE_TYPE("workerProbeType"),
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
    @SuppressWarnings("checkstyle:visibilitymodifier")
    public String workerProbeType = ProbesType.WORKER.getName();

    private final Object testClassInstance;
    private final Class testClassType;
    private final T testContext;
    private final ProbesConfiguration probesConfiguration;
    private final TestCase testCase;

    private PerformanceState lastPerformanceState;
    private final PerformanceTracker performanceTracker;
    private volatile boolean finished;

    private final Map<String, SimpleProbe<?, ?>> probeMap = new ConcurrentHashMap<String, SimpleProbe<?, ?>>();

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
    private Method messageConsumerMethod;

    private IWorker operationCountWorkerInstance;
    private ThreadSpawner workerThreadSpawner;

    public TestContainer(Object testObject, T testContext, ProbesConfiguration probesConfiguration, TestCase testCase) {
        if (testObject == null) {
            throw new NullPointerException();
        }
        if (testContext == null) {
            throw new NullPointerException();
        }

        this.testClassInstance = testObject;
        this.testClassType = testObject.getClass();
        this.testContext = testContext;
        this.probesConfiguration = probesConfiguration;
        this.testCase = testCase;
        this.performanceTracker = new PerformanceTracker();

        initMethods();
    }

    public Map<String, Result<?>> getProbeResults() {
        Map<String, Result<?>> results = new HashMap<String, Result<?>>(probeMap.size());
        for (Map.Entry<String, SimpleProbe<?, ?>> entry : probeMap.entrySet()) {
            String probeName = entry.getKey();
            SimpleProbe<?, ?> probe = entry.getValue();
            if (!probe.isDisabled()) {
                Result<?> result = probe.getResult();
                results.put(probeName, result);
            }
        }
        return results;
    }

    public T getTestContext() {
        return testContext;
    }

    public long getOperationCount() {
        try {
            Long count = invokeMethod((operationCountWorkerInstance != null) ? operationCountWorkerInstance : testClassInstance,
                    operationCountMethod);
            return (count == null ? -1 : count);
        } catch (Exception e) {
            LOGGER.debug("Exception while retrieving operation count from " + testCase.getId() + ": " + e.getMessage());
            return -1;
        }
    }

    public PerformanceState getPerformance() {
        if (!finished) {
            lastPerformanceState = performanceTracker.update(getOperationCount());
        }
        return lastPerformanceState;
    }

    public List<String> getStackTraces() throws Exception {
        if (workerThreadSpawner == null) {
            return Collections.emptyList();
        }
        return workerThreadSpawner.getStackTraces();
    }

    public void sendMessage(Message message) throws Exception {
        invokeMethod(testClassInstance, messageConsumerMethod, message);
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
        for (SimpleProbe probe : probeMap.values()) {
            probe.startProbing(now);
        }
        if (runWithWorkerMethod != null) {
            invokeRunWithWorkerMethod(now);
        } else {
            invokeMethod(testClassInstance, runMethod);
        }
        finished = true;
        now = Clock.currentTimeMillis();
        for (SimpleProbe probe : probeMap.values()) {
            probe.stopProbing(now);
        }
    }

    private void initMethods() {
        try {
            runMethod = getAtMostOneVoidMethodWithoutArgs(testClassType, Run.class);
            runWithWorkerMethod = getAtMostOneMethodWithoutArgs(testClassType, RunWithWorker.class, IWorker.class);
            if (!(runMethod == null ^ runWithWorkerMethod == null)) {
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
            messageConsumerMethod = getAtMostOneVoidMethodSkipArgsCheck(testClassType, Receive.class);
            if (messageConsumerMethod != null) {
                assertArguments(messageConsumerMethod, Message.class);
            }
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
            } else if (!parameterType.isAssignableFrom(IntervalProbe.class) || isObject) {
                illegalArgumentFound = true;
                break;
            }
        }
        if (!testContextFound || illegalArgumentFound) {
            throw new IllegalTestException(
                    format("Method %s.%s must have argument of type %s and zero or more arguments of type %s",
                            testClassType, method, TestContext.class, SimpleProbe.class));
        }
    }

    private void assertArguments(Method method, Class... arguments) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length != arguments.length) {
            throw new IllegalTestException(format("Method %s must have %s arguments, but %s arguments found",
                    method, arguments.length, parameterTypes.length));
        }

        for (int i = 0; i < arguments.length; i++) {
            if (!parameterTypes[i].isAssignableFrom(arguments[i])) {
                throw new IllegalTestException(format("Method %s has argument of type %s at index %d where type %s is expected",
                        method, parameterTypes[i], i + 1, arguments[i]));
            }
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
            if (SimpleProbe.class.equals(field.getType())) {
                SimpleProbe probe = getOrCreateConcurrentProbe(probeName, SimpleProbe.class);
                injectObjectToInstance(testClassInstance, field, probe);
            } else if (IntervalProbe.class.equals(field.getType())) {
                IntervalProbe probe = getOrCreateConcurrentProbe(probeName, IntervalProbe.class);
                injectObjectToInstance(testClassInstance, field, probe);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private <P extends SimpleProbe> P getOrCreateConcurrentProbe(String probeName, Class<P> targetClassType) {
        SimpleProbe<?, ?> probe = probeMap.get(probeName);
        if (probe == null) {
            probe = Probes.createConcurrentProbe(probeName, targetClassType, probesConfiguration);
            probeMap.put(probeName, probe);
            return (P) probe;
        }
        if (!probe.getClass().isAssignableFrom(targetClassType)) {
            throw new IllegalArgumentException(format("Probe \"%s\" of type %s does not match requested probe type %s",
                    probeName, probe.getClass().getSimpleName(), targetClassType.getSimpleName()));
        }
        return (P) probe;
    }

    private void invokeRunWithWorkerMethod(long now) throws Exception {
        bindOptionalProperty(this, testCase, OptionalTestProperties.THREAD_COUNT.propertyName);
        bindOptionalProperty(this, testCase, OptionalTestProperties.WORKER_PROBE_TYPE.propertyName);

        String testId = (testContext.getTestId().isEmpty() ? "Default" : testContext.getTestId());
        String probeName = testId + "IntervalProbe";

        if (ProbesType.getProbeType((workerProbeType)) == null) {
            LOGGER.warn("Illegal argument workerProbeType: " + workerProbeType);
            workerProbeType = ProbesType.WORKER.getName();
        }

        LOGGER.info(format("Spawning %d worker threads for test %s with %s probe", threadCount, testId, workerProbeType));
        if (threadCount <= 0) {
            return;
        }

        // create instance to get class of worker
        Class workerClass = invokeMethod(testClassInstance, runWithWorkerMethod).getClass();

        Field testContextField = getField(workerClass, "testContext", TestContext.class);
        Field intervalProbeField = getField(workerClass, "intervalProbe", IntervalProbe.class);

        Method optionalMethod = getAtMostOneMethodWithoutArgs(workerClass, Performance.class, Long.TYPE);
        if (optionalMethod != null) {
            operationCountMethod = optionalMethod;
        }

        // create one concurrent probe per test and inject it in all worker instances of the test
        probesConfiguration.addConfig(probeName, workerProbeType);
        IntervalProbe intervalProbe = getOrCreateConcurrentProbe(probeName, IntervalProbe.class);
        intervalProbe.startProbing(now);

        operationCountMethod = getAtMostOneMethodWithoutArgs(AbstractWorker.class, Performance.class, Long.TYPE);

        // spawn worker and wait for completion
        spawnWorkerThreads(testContextField, intervalProbeField, intervalProbe);

        // call the afterCompletion method on a single instance of the worker
        operationCountWorkerInstance.afterCompletion();
    }

    private void spawnWorkerThreads(Field testContextField, Field intervalProbeField, IntervalProbe intervalProbe)
            throws Exception {
        workerThreadSpawner = new ThreadSpawner(testContext.getTestId());
        for (int i = 0; i < threadCount; i++) {
            IWorker worker = invokeMethod(testClassInstance, runWithWorkerMethod);

            if (testContextField != null) {
                injectObjectToInstance(worker, testContextField, testContext);
            }
            if (intervalProbeField != null) {
                injectObjectToInstance(worker, intervalProbeField, intervalProbe);
            }

            bindOptionalProperty(worker, testCase, OptionalTestProperties.LOG_FREQUENCY.propertyName);

            operationCountWorkerInstance = worker;

            workerThreadSpawner.spawn(worker);
        }
        workerThreadSpawner.awaitCompletion();
    }

    boolean hasProbe(String probeName) {
        return probeMap.keySet().contains(probeName);
    }
}
