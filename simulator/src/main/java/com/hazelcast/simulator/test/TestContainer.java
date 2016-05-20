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
package com.hazelcast.simulator.test;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.probes.ProbeType;
import com.hazelcast.simulator.probes.impl.HdrProbe;
import com.hazelcast.simulator.probes.impl.ThroughputProbe;
import com.hazelcast.simulator.test.annotations.InjectHazelcastInstance;
import com.hazelcast.simulator.test.annotations.InjectMetronome;
import com.hazelcast.simulator.test.annotations.InjectProbe;
import com.hazelcast.simulator.test.annotations.InjectTestContext;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.utils.AnnotationFilter;
import com.hazelcast.simulator.utils.AnnotationFilter.TeardownFilter;
import com.hazelcast.simulator.utils.AnnotationFilter.VerifyFilter;
import com.hazelcast.simulator.utils.AnnotationFilter.WarmupFilter;
import com.hazelcast.simulator.utils.ThreadSpawner;
import com.hazelcast.simulator.worker.metronome.Metronome;
import com.hazelcast.simulator.worker.metronome.MetronomeType;
import com.hazelcast.simulator.worker.tasks.IMultipleProbesWorker;
import com.hazelcast.simulator.worker.tasks.IWorker;
import com.hazelcast.simulator.worker.tasks.WorkerThroughputProbe;
import org.apache.log4j.Logger;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.simulator.probes.ProbeType.HDR;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getAtMostOneMethodWithoutArgs;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getAtMostOneVoidMethodSkipArgsCheck;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getAtMostOneVoidMethodWithoutArgs;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getMetronomeIntervalMillis;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getMetronomeType;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getProbeName;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.isPartOfTotalThroughput;
import static com.hazelcast.simulator.utils.PropertyBindingSupport.bindProperties;
import static com.hazelcast.simulator.utils.PropertyBindingSupport.getPropertyValue;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokeMethod;
import static com.hazelcast.simulator.utils.ReflectionUtils.setFieldValue;
import static com.hazelcast.simulator.worker.metronome.MetronomeFactory.withFixedIntervalMs;
import static com.hazelcast.simulator.worker.metronome.MetronomeType.NOP;
import static com.hazelcast.simulator.worker.tasks.VeryAbstractWorker.DEFAULT_WORKER_PROBE_NAME;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.synchronizedSet;
import static org.apache.commons.lang3.text.WordUtils.capitalizeFully;

/**
 * Container for test class instances.
 *
 * <ul>
 * <li>Creates the test class instance by its fully qualified class name.</li>
 * <li>Binds properties to the test class instance (test parameters).</li>
 * <li>Injects required objects to annotated fields.</li>
 * <li>Analyses the test class instance for annotated test phase methods.</li>
 * <li>Provides a method to invoke test methods.</li>
 * </ul>
 */
public class TestContainer {

    static final String THREAD_COUNT_PROPERTY_NAME = "threadCount";
    static final String METRONOME_INTERVAL_PROPERTY_NAME = "metronomeInterval";
    static final String METRONOME_TYPE_PROPERTY_NAME = "metronomeType";
    static final String PROBE_TYPE_PROPERTY_NAME = "probeType";

    private static final int DEFAULT_RUN_WITH_WORKER_THREAD_COUNT = 10;
    private static final int DEFAULT_RUN_WITH_WORKER_METRONOME_INTERVAL = 0;
    private static final MetronomeType DEFAULT_RUN_WITH_WORKER_METRONOME_TYPE = NOP;
    private static final ProbeType DEFAULT_PROBE_TYPE = HDR;

    private static final Set<String> OPTIONAL_TEST_PROPERTIES = new HashSet<String>(asList(
            THREAD_COUNT_PROPERTY_NAME,
            METRONOME_INTERVAL_PROPERTY_NAME,
            METRONOME_TYPE_PROPERTY_NAME,
            PROBE_TYPE_PROPERTY_NAME
    ));

    private static final Logger LOGGER = Logger.getLogger(TestContainer.class);

    private final Map<String, Probe> probeMap = new ConcurrentHashMap<String, Probe>();
    private final Map<TestPhase, Method> testMethods = new HashMap<TestPhase, Method>();

    private final TestContext testContext;
    private final Object testClassInstance;
    private final Class testClassType;

    private final int runWithWorkerThreadCount;
    private final int runWithWorkerMetronomeInterval;
    private final MetronomeType runWithWorkerMetronomeType;
    private final ProbeType probeType;

    private boolean runWithWorker;
    private Object[] setupArguments;

    private long testStartedTimestamp;
    private volatile boolean isRunning;
    private final Set<IWorker> workers = synchronizedSet(new HashSet<IWorker>());

    public TestContainer(TestContext testContext, TestCase testCase) {
        this(testContext, getTestClassInstance(testCase), getThreadCount(testCase),
                getMetronomeIntervalProperty(testCase), getMetronomeTypeProperty(testCase),
                isLightweightProbe(testCase));
    }

    public TestContainer(TestContext testContext, Object testClassInstance) {
        this(testContext, testClassInstance, DEFAULT_RUN_WITH_WORKER_THREAD_COUNT,
                DEFAULT_RUN_WITH_WORKER_METRONOME_INTERVAL, DEFAULT_RUN_WITH_WORKER_METRONOME_TYPE, DEFAULT_PROBE_TYPE);
    }

    public TestContainer(TestContext testContext, Object testClassInstance, ProbeType probeType) {
        this(testContext, testClassInstance, DEFAULT_RUN_WITH_WORKER_THREAD_COUNT,
                DEFAULT_RUN_WITH_WORKER_METRONOME_INTERVAL, DEFAULT_RUN_WITH_WORKER_METRONOME_TYPE, probeType);
    }

    public TestContainer(TestContext testContext, Object testClassInstance, int runWithWorkerThreadCount) {
        this(testContext, testClassInstance, runWithWorkerThreadCount,
                DEFAULT_RUN_WITH_WORKER_METRONOME_INTERVAL, DEFAULT_RUN_WITH_WORKER_METRONOME_TYPE, DEFAULT_PROBE_TYPE);
    }

    public TestContainer(TestContext testContext, Object testClassInstance, int runWithWorkerThreadCount,
                         int runWithWorkerMetronomeInterval, MetronomeType runWithWorkerMetronomeType,
                         ProbeType probeType) {
        if (testContext == null) {
            throw new NullPointerException("testContext cannot be null!");
        }
        if (testClassInstance == null) {
            throw new NullPointerException("testClassInstance cannot be null!");
        }

        this.testContext = testContext;
        this.testClassInstance = testClassInstance;
        this.testClassType = testClassInstance.getClass();

        this.runWithWorkerThreadCount = runWithWorkerThreadCount;
        this.runWithWorkerMetronomeInterval = runWithWorkerMetronomeInterval;
        this.runWithWorkerMetronomeType = runWithWorkerMetronomeType;
        this.probeType = probeType;

        injectDependencies();
        initTestMethods();
    }

    public Object getTestInstance() {
        return testClassInstance;
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
            case RUN:
                invokeRun();
                break;
            case SETUP:
                invokeMethod(testClassInstance, testMethods.get(TestPhase.SETUP), setupArguments);
                break;
            default:
                invokeMethod(testClassInstance, testMethods.get(testPhase));
        }
    }

    // just for testing
    boolean hasProbe(String probeName) {
        return probeMap.containsKey(probeName);
    }

    private void injectDependencies() {
        Map<Field, Object> injectMap = getInjectMap(testClassType);
        injectObjects(injectMap, testClassInstance);
    }

    private void initTestMethods() {
        Method runMethod;
        Method runWithWorkerMethod;
        try {
            runMethod = getAtMostOneVoidMethodWithoutArgs(testClassType, Run.class);
            runWithWorkerMethod = getAtMostOneMethodWithoutArgs(testClassType, RunWithWorker.class, IWorker.class);
            if (runWithWorkerMethod != null) {
                runWithWorker = true;
                testMethods.put(TestPhase.RUN, runWithWorkerMethod);
            } else {
                testMethods.put(TestPhase.RUN, runMethod);
            }

            Method setupMethod = getAtMostOneVoidMethodSkipArgsCheck(testClassType, Setup.class);
            if (setupMethod != null) {
                setupArguments = getSetupArguments(setupMethod);
                testMethods.put(TestPhase.SETUP, setupMethod);
            }

            setTestMethod(Warmup.class, new WarmupFilter(false), TestPhase.LOCAL_WARMUP);
            setTestMethod(Warmup.class, new WarmupFilter(true), TestPhase.GLOBAL_WARMUP);

            setTestMethod(Verify.class, new VerifyFilter(false), TestPhase.LOCAL_VERIFY);
            setTestMethod(Verify.class, new VerifyFilter(true), TestPhase.GLOBAL_VERIFY);

            setTestMethod(Teardown.class, new TeardownFilter(false), TestPhase.LOCAL_TEARDOWN);
            setTestMethod(Teardown.class, new TeardownFilter(true), TestPhase.GLOBAL_TEARDOWN);
        } catch (Exception e) {
            throw new IllegalTestException("Error during search for annotated test methods in" + testClassType.getName(), e);
        }
        if ((runMethod == null) == (runWithWorkerMethod == null)) {
            throw new IllegalTestException(format("Test must contain either %s or %s method", Run.class, RunWithWorker.class));
        }
    }

    private Object[] getSetupArguments(Method setupMethod) {
        Class[] parameterTypes = setupMethod.getParameterTypes();
        Object[] arguments = new Object[parameterTypes.length];
        if (parameterTypes.length < 1) {
            return arguments;
        }

        for (int i = 0; i < parameterTypes.length; i++) {
            Class<?> parameterType = parameterTypes[i];
            if (!parameterType.isAssignableFrom(TestContext.class) || parameterType.isAssignableFrom(Object.class)) {
                throw new IllegalTestException(format("Method %s.%s() supports arguments of type %s, but found %s at position %d",
                        testClassType.getSimpleName(), setupMethod, TestContext.class.getName(), parameterType.getName(), i));
            }
            arguments[i] = testContext;
        }
        return arguments;
    }

    private void setTestMethod(Class<? extends Annotation> annotationClass, AnnotationFilter filter, TestPhase testPhase) {
        Method method = getAtMostOneVoidMethodWithoutArgs(testClassType, annotationClass, filter);
        testMethods.put(testPhase, method);
    }

    private void invokeRun() throws Exception {
        try {
            Method method = testMethods.get(TestPhase.RUN);
            if (runWithWorker) {
                invokeRunWithWorkerMethod(method);
            } else {
                testStartedTimestamp = System.currentTimeMillis();
                isRunning = true;
                invokeMethod(testClassInstance, method);
            }
        } finally {
            isRunning = false;
        }
    }

    private void invokeRunWithWorkerMethod(Method runMethod) throws Exception {
        LOGGER.info(format("Spawning %d worker threads for test %s", runWithWorkerThreadCount, testContext.getTestId()));
        if (runWithWorkerThreadCount <= 0) {
            return;
        }

        // create instance to get the class of the IWorker implementation
        IWorker workerInstance = invokeMethod(testClassInstance, runMethod);
        Class<? extends IWorker> workerClass = workerInstance.getClass();

        Map<Field, Object> injectMap = getInjectMap(workerClass);
        Map<Enum, Probe> operationProbeMap = getOperationProbeMap(workerClass, workerInstance);

        // everything is prepared, we can notify the outside world now
        testStartedTimestamp = System.currentTimeMillis();
        isRunning = true;

        // spawn workers and wait for completion
        IWorker worker = runWorkers(runWithWorkerThreadCount, runMethod, injectMap, operationProbeMap);

        // call the afterCompletion() method on a single instance of the worker
        worker.afterCompletion();
    }

    private Map<Field, Object> getInjectMap(Class classType) {
        Class clazz = classType;
        Map<Field, Object> injectMap = new HashMap<Field, Object>();
        do {
            for (Field field : clazz.getDeclaredFields()) {
                Class fieldType = field.getType();
                if (field.isAnnotationPresent(InjectTestContext.class)) {
                    assertFieldType(fieldType, TestContext.class, InjectTestContext.class);
                    injectMap.put(field, testContext);
                } else if (field.isAnnotationPresent(InjectHazelcastInstance.class)) {
                    assertFieldType(fieldType, HazelcastInstance.class, InjectHazelcastInstance.class);
                    injectMap.put(field, testContext.getTargetInstance());
                } else if (field.isAnnotationPresent(InjectProbe.class)) {
                    assertFieldType(fieldType, Probe.class, InjectProbe.class);
                    Probe probe = getOrCreateProbe(getProbeName(field), isPartOfTotalThroughput(field));
                    injectMap.put(field, probe);
                } else if (field.isAnnotationPresent(InjectMetronome.class)) {
                    assertFieldType(fieldType, Metronome.class, InjectMetronome.class);
                    int intervalMillis = getMetronomeIntervalMillis(field, runWithWorkerMetronomeInterval);
                    MetronomeType type = getMetronomeType(field, runWithWorkerMetronomeType);
                    injectMap.put(field, withFixedIntervalMs(intervalMillis, type));
                }
            }
            clazz = clazz.getSuperclass();
        } while (clazz != null);
        return injectMap;
    }

    private Map<Enum, Probe> getOperationProbeMap(Class<? extends IWorker> workerClass, IWorker worker) {
        if (!IMultipleProbesWorker.class.isAssignableFrom(workerClass)) {
            return null;
        }

        // remove the default worker probe
        probeMap.remove(DEFAULT_WORKER_PROBE_NAME);

        Map<Enum, Probe> operationProbes = new HashMap<Enum, Probe>();
        for (Enum operation : ((IMultipleProbesWorker) worker).getOperations()) {
            String probeName = capitalizeFully(operation.name(), '_').replace("_", "") + "Probe";
            operationProbes.put(operation, getOrCreateProbe(probeName, true));
        }
        return operationProbes;
    }

    private Probe getOrCreateProbe(String probeName, boolean partOfTotalThroughput) {
        Probe probe = probeMap.get(probeName);

        if (probe == null) {
            switch (probeType) {
                case HDR:
                    probe = new HdrProbe(partOfTotalThroughput);
                    break;
                case LATENCY:
                    if (probeName.equals(DEFAULT_WORKER_PROBE_NAME)) {
                        probe = new WorkerThroughputProbe(workers);
                    } else {
                        probe = new ThroughputProbe(partOfTotalThroughput);
                    }

                    break;
                default:
                    throw new IllegalArgumentException("unknown probeType:" + probeType);
            }
            probeMap.put(probeName, probe);
        }

        return probe;
    }

    private IWorker runWorkers(int threadCount, Method runMethod, Map<Field, Object> injectMap,
                               Map<Enum, Probe> operationProbes) throws Exception {
        IWorker firstWorker = null;
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int i = 0; i < threadCount; i++) {
            final IWorker worker = invokeMethod(testClassInstance, runMethod);
            workers.add(worker);

            if (firstWorker == null) {
                firstWorker = worker;
            }

            injectObjects(injectMap, worker);
            if (operationProbes != null) {
                ((IMultipleProbesWorker) worker).setProbeMap(operationProbes);
            }
            spawner.spawn(new WorkerTask(worker));
        }
        spawner.awaitCompletion();
        return firstWorker;
    }

    private static Object getTestClassInstance(TestCase testCase) {
        if (testCase == null) {
            throw new NullPointerException();
        }
        String classname = testCase.getClassname();
        Object testObject;
        try {
            ClassLoader classLoader = TestContainer.class.getClassLoader();
            testObject = classLoader.loadClass(classname).newInstance();
        } catch (Exception e) {
            throw new IllegalTestException("Could not create instance of " + classname, e);
        }
        bindProperties(testObject, testCase, OPTIONAL_TEST_PROPERTIES);
        return testObject;
    }

    private static int getThreadCount(TestCase testCase) {
        String propertyValue = getPropertyValue(testCase, THREAD_COUNT_PROPERTY_NAME);
        return propertyValue == null ? DEFAULT_RUN_WITH_WORKER_THREAD_COUNT : parseInt(propertyValue);
    }

    private static int getMetronomeIntervalProperty(TestCase testCase) {
        String propertyValue = getPropertyValue(testCase, METRONOME_INTERVAL_PROPERTY_NAME);
        return propertyValue == null ? DEFAULT_RUN_WITH_WORKER_METRONOME_INTERVAL : parseInt(propertyValue);
    }

    private static MetronomeType getMetronomeTypeProperty(TestCase testCase) {
        String propertyValue = getPropertyValue(testCase, METRONOME_TYPE_PROPERTY_NAME);
        return propertyValue == null ? DEFAULT_RUN_WITH_WORKER_METRONOME_TYPE : MetronomeType.valueOf(propertyValue);
    }

    private static ProbeType isLightweightProbe(TestCase testCase) {
        String propertyValue = getPropertyValue(testCase, PROBE_TYPE_PROPERTY_NAME);
        return propertyValue == null ? DEFAULT_PROBE_TYPE : ProbeType.valueOf(propertyValue);
    }

    private static void assertFieldType(Class fieldType, Class expectedFieldType, Class<? extends Annotation> annotation) {
        if (!expectedFieldType.equals(fieldType)) {
            throw new IllegalTestException(format("Found %s annotation on field of type %s, but %s is required!",
                    annotation.getName(), fieldType.getName(), expectedFieldType.getName()));
        }
    }

    private static void injectObjects(Map<Field, Object> injectMap, Object worker) {
        for (Map.Entry<Field, Object> entry : injectMap.entrySet()) {
            setFieldValue(worker, entry.getKey(), entry.getValue());
        }
    }
}
