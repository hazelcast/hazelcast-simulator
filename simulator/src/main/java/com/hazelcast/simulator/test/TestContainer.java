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

import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.utils.AnnotationFilter;
import com.hazelcast.simulator.utils.AnnotationFilter.TeardownFilter;
import com.hazelcast.simulator.utils.AnnotationFilter.VerifyFilter;
import com.hazelcast.simulator.utils.AnnotationFilter.WarmupFilter;
import com.hazelcast.simulator.worker.PrimordialRunStrategy;
import com.hazelcast.simulator.worker.RunStrategy;
import com.hazelcast.simulator.worker.RunWithWorkersRunStrategy;
import com.hazelcast.simulator.worker.TimeStepRunStrategy;
import com.hazelcast.simulator.worker.tasks.IWorker;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static com.hazelcast.simulator.test.TestPhase.GLOBAL_TEARDOWN;
import static com.hazelcast.simulator.test.TestPhase.GLOBAL_VERIFY;
import static com.hazelcast.simulator.test.TestPhase.GLOBAL_WARMUP;
import static com.hazelcast.simulator.test.TestPhase.LOCAL_TEARDOWN;
import static com.hazelcast.simulator.test.TestPhase.LOCAL_VERIFY;
import static com.hazelcast.simulator.test.TestPhase.LOCAL_WARMUP;
import static com.hazelcast.simulator.test.TestPhase.RUN;
import static com.hazelcast.simulator.test.TestPhase.SETUP;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.findAllMethods;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getAtMostOneMethodWithoutArgs;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getAtMostOneVoidMethod;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getAtMostOneVoidMethodWithoutArgs;
import static com.hazelcast.simulator.utils.Preconditions.checkNotNull;
import static java.lang.String.format;

/**
 * Container for test class instances.
 * <p>
 * <ul>
 * <li>Creates the test class instance by its fully qualified class name.</li>
 * <li>Binds properties to the test class instance (test parameters).</li>
 * <li>Injects required objects to annotated fields.</li>
 * <li>Analyses the test class instance for annotated test phase methods.</li>
 * <li>Provides a method to invoke test methods.</li>
 * </ul>
 */
public class TestContainer {

    private final TestContext testContext;
    private final TestCase testCase;
    private final Object testInstance;
    private final Map<TestPhase, Callable> taskPerPhaseMap = new HashMap<TestPhase, Callable>();
    private final DependencyInjector dependencyInjector;
    private final Class testInstanceClass;

    public TestContainer(TestContext testContext, TestCase testCase) {
        this(testContext, null, testCase);
    }

    public TestContainer(TestContext testContext, Object givenTestInstance, TestCase testCase) {
        this.testContext = checkNotNull(testContext, "testContext can't null!");
        this.testCase = checkNotNull(testCase, "testCase can't be null!");
        this.dependencyInjector = new DependencyInjector(testContext, testCase);

        dependencyInjector.inject(this);

        if (givenTestInstance == null) {
            this.testInstance = newTestInstance();
        } else {
            this.testInstance = givenTestInstance;
        }
        this.testInstanceClass = testInstance.getClass();
        dependencyInjector.inject(testInstance);

        registerTestPhaseTasks();

        dependencyInjector.ensureNoUnusedProperties();
    }

    private Object newTestInstance() {
        String testInstanceClassName = testCase.getClassname();
        try {
            Class testInstanceClass = TestContainer.class.getClassLoader().loadClass(testInstanceClassName);
            return testInstanceClass.newInstance();
        } catch (Exception e) {
            throw new IllegalTestException("Could not create instance of " + testInstanceClassName, e);
        }
    }

    public DependencyInjector getDependencyInjector() {
        return dependencyInjector;
    }

    public Object getTestInstance() {
        return testInstance;
    }

    public TestContext getTestContext() {
        return testContext;
    }

    public TestCase getTestCase() {
        return testCase;
    }

    public long getTestStartedTimestamp() {
        RunStrategy runStrategy = (RunStrategy) taskPerPhaseMap.get(RUN);
        return runStrategy == null ? 0 : runStrategy.getStartedTimestamp();
    }

    public boolean isRunning() {
        RunStrategy runStrategy = (RunStrategy) taskPerPhaseMap.get(RUN);
        return runStrategy == null ? false : runStrategy.isRunning();
    }

    public long iteration() {
        RunStrategy runStrategy = (RunStrategy) taskPerPhaseMap.get(RUN);
        return runStrategy == null ? 0 : runStrategy.iterations();
    }

    public Map<String, Probe> getProbeMap() {
        return dependencyInjector.getProbeMap();
    }

    public void invoke(TestPhase testPhase) throws Exception {
        Callable task = taskPerPhaseMap.get(testPhase);
        if (task == null) {
            return;
        }

        task.call();
    }

    private void registerTestPhaseTasks() {
        try {
            registerSetupTask();

            registerTask(Warmup.class, new WarmupFilter(false), LOCAL_WARMUP);
            registerTask(Warmup.class, new WarmupFilter(true), GLOBAL_WARMUP);

            registerRunStrategyTask();

            registerTask(Verify.class, new VerifyFilter(false), LOCAL_VERIFY);
            registerTask(Verify.class, new VerifyFilter(true), GLOBAL_VERIFY);

            registerTask(Teardown.class, new TeardownFilter(false), LOCAL_TEARDOWN);
            registerTask(Teardown.class, new TeardownFilter(true), GLOBAL_TEARDOWN);
        } catch (IllegalTestException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalTestException("Error during search for annotated test methods in" + testInstanceClass.getName(), e);
        }
    }

    private void registerSetupTask() {
        Method setupMethod = getAtMostOneVoidMethod(testInstanceClass, Setup.class);
        if (setupMethod != null) {
            Object[] args = getSetupArguments(setupMethod);
            taskPerPhaseMap.put(SETUP, new MethodInvokingCallable(testInstance, setupMethod, args));
        }
    }

    private Object[] getSetupArguments(Method setupMethod) {
        Class[] parameterTypes = setupMethod.getParameterTypes();
        Object[] arguments = new Object[parameterTypes.length];
        if (parameterTypes.length == 0) {
            return arguments;
        }

        for (int i = 0; i < parameterTypes.length; i++) {
            Class<?> parameterType = parameterTypes[i];
            if (!parameterType.isAssignableFrom(TestContext.class) || parameterType.isAssignableFrom(Object.class)) {
                throw new IllegalTestException(format("Method %s.%s() supports arguments of type %s, but found %s at position %d",
                        testInstanceClass.getSimpleName(), setupMethod, TestContext.class.getName(), parameterType.getName(), i));
            }
            arguments[i] = testContext;
        }
        return arguments;
    }

    private void registerRunStrategyTask() {
        Method runMethod = getAtMostOneVoidMethodWithoutArgs(testInstanceClass, Run.class);
        List<RunStrategy> runStrategies = new LinkedList<RunStrategy>();
        if (runMethod != null) {
            runStrategies.add(new PrimordialRunStrategy(testInstance, runMethod));
        }

        Method runWithWorker = getAtMostOneMethodWithoutArgs(testInstanceClass, RunWithWorker.class, IWorker.class);
        if (runWithWorker != null) {
            runStrategies.add(new RunWithWorkersRunStrategy(this, runWithWorker));
        }

        List<Method> timeStepMethods = findAllMethods(testInstanceClass, TimeStep.class);
        if (timeStepMethods.size() > 0) {
            runStrategies.add(new TimeStepRunStrategy(this));
        }

        if (runStrategies.isEmpty()) {
            throw new IllegalTestException(format("Test must contain either %s or %s or %s method",
                            Run.class, RunWithWorker.class, TimeStep.class));
        }

        if (runStrategies.size() > 1) {
            throw new IllegalTestException("Test must contain a single run strategy.");
        }

        taskPerPhaseMap.put(RUN, runStrategies.get(0));
    }

    private void registerTask(Class<? extends Annotation> annotationClass, AnnotationFilter filter, TestPhase testPhase) {
        Method method = getAtMostOneVoidMethodWithoutArgs(testInstanceClass, annotationClass, filter);
        if (method != null) {
            taskPerPhaseMap.put(testPhase, new MethodInvokingCallable(testInstance, method));
        }
    }

}
