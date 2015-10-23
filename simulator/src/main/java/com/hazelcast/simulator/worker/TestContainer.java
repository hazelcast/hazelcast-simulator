/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.worker;

import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.probes.impl.ProbeImpl;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestPhase;
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
import com.hazelcast.simulator.worker.tasks.IWorker;
import org.apache.log4j.Logger;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getAtMostOneMethodWithoutArgs;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getAtMostOneVoidMethodSkipArgsCheck;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getAtMostOneVoidMethodWithoutArgs;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getProbeName;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.isThroughputProbe;
import static com.hazelcast.simulator.utils.PropertyBindingSupport.bindOptionalProperty;
import static com.hazelcast.simulator.utils.ReflectionUtils.getField;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokeMethod;
import static com.hazelcast.simulator.utils.ReflectionUtils.setFieldValue;
import static java.lang.String.format;

/**
 * Since the test is based on annotations there is no API we can call easily.
 * That is the task of this test container.
 */
public class TestContainer {

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

        public String getPropertyName() {
            return propertyName;
        }
    }

    static {
        Set<String> optionalTestProperties = new HashSet<String>();
        for (OptionalTestProperties optionalTestProperty : OptionalTestProperties.values()) {
            optionalTestProperties.add(optionalTestProperty.getPropertyName());
        }
        OPTIONAL_TEST_PROPERTIES = Collections.unmodifiableSet(optionalTestProperties);
    }

    // properties
    @SuppressWarnings("checkstyle:visibilitymodifier")
    public int threadCount = DEFAULT_THREAD_COUNT;

    private final Map<String, Probe> probeMap = new ConcurrentHashMap<String, Probe>();

    private final Object testClassInstance;
    private final Class testClassType;
    private final TestContext testContext;
    private final TestCase testCase;

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

    private long testStartedTimestamp;
    private volatile boolean isRunning;

    public TestContainer(Object testObject, TestContext testContext, TestCase testCase) {
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

        initMethods();
    }

    public TestContext getTestContext() {
        return testContext;
    }

    public long getTestStartedTimestamp() {
        return testStartedTimestamp;
    }

    public boolean isRunning() {
        return isRunning;
    }

    public Map<String, Probe> getProbeMap() {
        return probeMap;
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
        testStartedTimestamp = System.currentTimeMillis();
        isRunning = true;
        if (runWithWorkerMethod != null) {
            invokeRunWithWorkerMethod();
        } else {
            invokeMethod(testClassInstance, runMethod);
        }
        isRunning = false;
    }

    private void initMethods() {
        try {
            runMethod = getAtMostOneVoidMethodWithoutArgs(testClassType, Run.class);
            runWithWorkerMethod = getAtMostOneMethodWithoutArgs(testClassType, RunWithWorker.class, IWorker.class);

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
        } catch (Exception e) {
            throw new IllegalTestException(e);
        }
        if ((runMethod == null) == (runWithWorkerMethod == null)) {
            throw new IllegalTestException(format("Test must contain either %s or %s method", Run.class, RunWithWorker.class));
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
            if (Probe.class.equals(field.getType())) {
                String probeName = getProbeName(field);
                Probe probe = getOrCreateProbe(probeName, field);
                setFieldValue(testClassInstance, field, probe);
            }
        }
    }

    private Probe getOrCreateProbe(String probeName, Field field) {
        Probe probe = probeMap.get(probeName);
        if (probe == null) {
            probe = new ProbeImpl(isThroughputProbe(field));
            probeMap.put(probeName, probe);
        }
        return probe;
    }

    private void invokeRunWithWorkerMethod() throws Exception {
        bindOptionalProperty(this, testCase, OptionalTestProperties.THREAD_COUNT.getPropertyName());

        LOGGER.info(format("Spawning %d worker threads for test %s", threadCount, testContext.getTestId()));
        if (threadCount <= 0) {
            return;
        }

        // create instance to get class of worker
        Class workerClass = invokeMethod(testClassInstance, runWithWorkerMethod).getClass();

        Field testContextField = getField(workerClass, "testContext", TestContext.class);
        Field workerProbeField = getField(workerClass, "workerProbe", Probe.class);

        Probe probe = null;
        if (workerProbeField != null) {
            // create one probe per test and inject it in all worker instances of the test
            probe = getOrCreateProbe(testContext.getTestId() + "WorkerProbe", workerProbeField);
        }

        // spawn worker and wait for completion
        IWorker worker = spawnWorkerThreads(testContextField, workerProbeField, probe);

        // call the afterCompletion method on a single instance of the worker
        if (worker != null) {
            worker.afterCompletion();
        }
    }

    private IWorker spawnWorkerThreads(Field testContextField, Field workerProbeField, Probe probe) throws Exception {
        IWorker worker = null;

        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int i = 0; i < threadCount; i++) {
            worker = invokeMethod(testClassInstance, runWithWorkerMethod);

            if (testContextField != null) {
                setFieldValue(worker, testContextField, testContext);
            }
            if (workerProbeField != null) {
                setFieldValue(worker, workerProbeField, probe);
            }

            bindOptionalProperty(worker, testCase, OptionalTestProperties.LOG_FREQUENCY.getPropertyName());

            spawner.spawn(worker);
        }
        spawner.awaitCompletion();

        return worker;
    }

    boolean hasProbe(String probeName) {
        return probeMap.keySet().contains(probeName);
    }
}
