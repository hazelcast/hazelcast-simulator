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
package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.AfterWarmup;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.utils.AnnotatedMethodRetriever;
import com.hazelcast.simulator.utils.AnnotationFilter;
import com.hazelcast.simulator.utils.AnnotationFilter.TeardownFilter;
import com.hazelcast.simulator.utils.AnnotationFilter.VerifyFilter;
import com.hazelcast.simulator.worker.performance.TestPerformanceTracker;
import com.hazelcast.simulator.worker.tasks.IWorker;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static com.hazelcast.simulator.common.TestPhase.GLOBAL_PREPARE;
import static com.hazelcast.simulator.common.TestPhase.GLOBAL_TEARDOWN;
import static com.hazelcast.simulator.common.TestPhase.GLOBAL_VERIFY;
import static com.hazelcast.simulator.common.TestPhase.LOCAL_PREPARE;
import static com.hazelcast.simulator.common.TestPhase.LOCAL_TEARDOWN;
import static com.hazelcast.simulator.common.TestPhase.LOCAL_VERIFY;
import static com.hazelcast.simulator.common.TestPhase.RUN;
import static com.hazelcast.simulator.common.TestPhase.SETUP;
import static com.hazelcast.simulator.utils.CommonUtils.rethrow;
import static com.hazelcast.simulator.utils.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.util.Arrays.asList;

/**
 * Container for test instances.
 * <p>
 * It is responsible for:
 * <ul>
 * <li>Creates the test class instance by its fully qualified class name.</li>
 * <li>Binding properties to the test class instance (test parameters).</li>
 * <li>Injects required objects to annotated fields.</li>
 * <li>Analyses the test class instance for annotated test phase methods.</li>
 * <li>Provides a method to invoke test methods.</li>
 * </ul>
 */
public class TestContainer {

    private final TestContextImpl testContext;
    private final TestCase testCase;
    private final Object testInstance;
    private final Map<TestPhase, Callable> taskPerPhaseMap = new HashMap<TestPhase, Callable>();
    private final PropertyBinding propertyBinding;
    private final Class testClass;
    private final RunStrategy runStrategy;
    private final TestPerformanceTracker testPerformanceTracker;

    public TestContainer(TestContextImpl targetInstance, TestCase testCase) {
        this(targetInstance, null, testCase);
    }

    public TestContainer(TestContextImpl testContext, Object givenTestInstance, TestCase testCase) {
        this.testContext = checkNotNull(testContext, "testContext can't null!");
        this.testCase = checkNotNull(testCase, "testCase can't be null!");
        this.propertyBinding = new PropertyBinding(testCase)
                .setTestContext(testContext);

        propertyBinding.bind(this);

        if (givenTestInstance == null) {
            this.testInstance = newTestInstance();
        } else {
            this.testInstance = givenTestInstance;
        }
        this.testClass = testInstance.getClass();
        propertyBinding.bind(testInstance);

        this.runStrategy = loadRunStrategy();

        registerTestPhaseTasks();

        propertyBinding.ensureNoUnusedProperties();

        this.testPerformanceTracker = new TestPerformanceTracker(this);
    }

    public TestPerformanceTracker getTestPerformanceTracker() {
        return testPerformanceTracker;
    }

    @SuppressWarnings({"unchecked", "PMD.PreserveStackTrace"})
    private Object newTestInstance() {
        String testClassName = testCase.getClassname();
        try {
            Class testClazz = TestContainer.class.getClassLoader().loadClass(testClassName);
            Constructor constructor = testClazz.getConstructor();
            return constructor.newInstance();
        } catch (IllegalTestException e) {
            throw rethrow(e);
        } catch (NoSuchMethodException e) {
            throw new IllegalTestException(format("Test class '%s' should have a public no arg constructor", testClassName));
        } catch (Exception e) {
            throw new IllegalTestException("Could not create instance of " + testClassName, e);
        }
    }

    public PropertyBinding getPropertyBinding() {
        return propertyBinding;
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

    public long getRunStartedMillis() {
        return runStrategy == null ? 0 : runStrategy.getStartedMillis();
    }

    public boolean isRunning() {
        return runStrategy == null ? false : runStrategy.isRunning();
    }

    public long iteration() {
        return runStrategy == null ? 0 : runStrategy.iterations();
    }

    public Map<String, Probe> getProbeMap() {
        return propertyBinding.getProbeMap();
    }

    public void invoke(TestPhase testPhase) throws Exception {
        Callable task = taskPerPhaseMap.get(testPhase);
        if (task == null) {
            return;
        }

        try {
            task.call();
        } catch (InvocationTargetException e) {
            Throwable t = e.getTargetException();
            if (t instanceof Error) {
                throw (Error) t;
            } else if (t instanceof Exception) {
                throw (Exception) t;
            } else {
                throw e;
            }
        }
    }

    private void registerTestPhaseTasks() {
        try {
            registerSetupTask();

            registerPrepareTasks(false);
            registerPrepareTasks(true);

            taskPerPhaseMap.put(RUN, runStrategy.getRunCallable());

            registerTask(Verify.class, new VerifyFilter(false), LOCAL_VERIFY);
            registerTask(Verify.class, new VerifyFilter(true), GLOBAL_VERIFY);

            registerTask(Teardown.class, new TeardownFilter(false), LOCAL_TEARDOWN);
            registerTask(Teardown.class, new TeardownFilter(true), GLOBAL_TEARDOWN);
        } catch (IllegalTestException e) {
            throw rethrow(e);
        } catch (Exception e) {
            throw new IllegalTestException(format("Error during search for annotated test methods in %s: [%s] %s",
                    testClass.getName(), e.getClass().getSimpleName(), e.getMessage()), e);
        }
    }

    private void registerSetupTask() {
        List<Method> setupMethods = new AnnotatedMethodRetriever(testClass, Setup.class)
                .withVoidReturnType()
                .withPublicNonStaticModifier()
                .findAll();

        List<Callable> callableList = new ArrayList<Callable>(setupMethods.size());
        for (Method setupMethod : setupMethods) {
            Class[] parameterTypes = setupMethod.getParameterTypes();

            Object[] args;
            switch (parameterTypes.length) {
                case 0:
                    args = new Object[0];
                    break;
                case 1:
                    Class<?> parameterType = setupMethod.getParameterTypes()[0];
                    if (!parameterType.isAssignableFrom(TestContext.class) || parameterType.isAssignableFrom(Object.class)) {
                        throw new IllegalTestException(format("Method %s.%s() supports arguments of type %s, but found %s",
                                testClass.getSimpleName(), setupMethod, TestContext.class.getName(), parameterType.getName()));
                    }
                    args = new Object[]{testContext};
                    break;
                default:
                    throw new IllegalTestException(format("Setup method '%s' can have at most a single argument", setupMethod));
            }

            callableList.add(new MethodInvokingCallable(testInstance, setupMethod, args));
        }


        taskPerPhaseMap.put(SETUP, new CompositeCallable(callableList));
    }

    private RunStrategy loadRunStrategy() {
        try {
            List<String> runAnnotations = new LinkedList<String>();
            RunStrategy runStrategy = null;

            Method runMethod = new AnnotatedMethodRetriever(testClass, Run.class)
                    .withoutArgs()
                    .withVoidReturnType()
                    .withPublicNonStaticModifier()
                    .find();
            if (runMethod != null) {
                assertNoResetMethods(Run.class);
                runAnnotations.add(Run.class.getName());
                runStrategy = new PrimordialRunStrategy(testInstance, runMethod);
            }

            Method runWithWorker = new AnnotatedMethodRetriever(testClass, RunWithWorker.class)
                    .withReturnType(IWorker.class)
                    .withoutArgs()
                    .withPublicNonStaticModifier()
                    .find();
            if (runWithWorker != null) {
                assertNoResetMethods(Run.class);
                runAnnotations.add(RunWithWorker.class.getName());
                runStrategy = new RunWithWorkersRunStrategy(this, runWithWorker);
            }

            List<Method> timeStepMethods = new AnnotatedMethodRetriever(testClass, TimeStep.class)
                    .findAll();
            if (!timeStepMethods.isEmpty()) {
                runAnnotations.add(TimeStep.class.getName());
                runStrategy = new TimeStepRunStrategy(this);
            }

            if (runAnnotations.size() == 0) {
                throw new IllegalTestException(
                        "Test is missing a run strategy, it must contain one of the following annotations: "
                                + asList(Run.class.getName(), RunWithWorker.class.getName(), TimeStep.class.getName()));
            } else if (runAnnotations.size() > 1) {
                throw new IllegalTestException("Test has more than one run strategy, found the following annotations: "
                        + runAnnotations);
            } else {
                return runStrategy;
            }
        } catch (IllegalTestException e) {
            throw rethrow(e);
        } catch (Exception e) {
            throw new IllegalTestException(format("Error loading run strategy in %s: [%s] %s",
                    testClass.getName(), e.getClass().getSimpleName(), e.getMessage()), e);
        }
    }

    private void assertNoResetMethods(Class<? extends Annotation> runAnnotation) {
        List<Method> methods = new AnnotatedMethodRetriever(testClass, AfterWarmup.class).findAll();
        if (methods.size() > 0) {
            throw new IllegalTestException(
                    format("Can't have reset '%s' in class '%s' since it uses '%s'",
                            methods.get(0), testClass.getName(), runAnnotation.getName()));
        }
    }

    private void registerTask(Class<? extends Annotation> annotationClass, AnnotationFilter filter, TestPhase testPhase) {
        List<Method> methods = new AnnotatedMethodRetriever(testClass, annotationClass)
                .withoutArgs()
                .withPublicNonStaticModifier()
                .withFilter(filter)
                .findAll();

        taskPerPhaseMap.put(testPhase, toCallable(methods));
    }

    private void registerPrepareTasks(boolean global) {
        List<Method> localPrepareMethods = new AnnotatedMethodRetriever(testClass, Prepare.class)
                .withoutArgs()
                .withPublicNonStaticModifier()
                .withFilter(new AnnotationFilter.PrepareFilter(global))
                .findAll();

        taskPerPhaseMap.put(global ? GLOBAL_PREPARE : LOCAL_PREPARE, toCallable(localPrepareMethods));
    }

    private Callable toCallable(List<Method> methods) {
        List<Callable> callableList = new ArrayList<Callable>(methods.size());
        for (Method method : methods) {
            callableList.add(new MethodInvokingCallable(testInstance, method));
        }
        return new CompositeCallable(callableList);
    }
}
