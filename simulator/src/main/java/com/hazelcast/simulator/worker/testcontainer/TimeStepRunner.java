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


import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.StopException;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.worker.metronome.Metronome;
import org.apache.log4j.Logger;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.simulator.utils.CommonUtils.rethrow;
import static java.lang.String.format;

/**
 * Simulator uses a code generator to generate a subclass of this class.
 */
public abstract class TimeStepRunner implements Runnable {

    protected TestContext testContext;
    protected Metronome metronome;

    protected final Logger logger = Logger.getLogger(getClass());
    protected final String executionGroup;
    protected final Object threadState;
    protected final Object testInstance;
    protected final AtomicLong iterations = new AtomicLong();
    protected final TimeStepModel timeStepModel;
    protected final byte[] timeStepProbabilities;
    protected final Map<String, Probe> probeMap = new HashMap<>();
    protected long maxIterations;
    protected long delayMillis;

    // There are used to prevent dead code optimization
    protected final AtomicReference atomicReference = new AtomicReference();
    protected final AtomicBoolean atomicBoolean = new AtomicBoolean();
    protected final AtomicInteger atomicInteger = new AtomicInteger();
    protected final AtomicLong atomicLong = new AtomicLong();

    public TimeStepRunner(Object testInstance, TimeStepModel timeStepModel, String executionGroup) {
        this.testInstance = testInstance;
        this.timeStepModel = timeStepModel;
        this.executionGroup = executionGroup;
        this.threadState = initThreadState();
        this.timeStepProbabilities = timeStepModel.getTimeStepProbabilityArray(executionGroup);
    }

    public String getExecutionGroup() {
        return executionGroup;
    }

    public void bind(PropertyBinding binding) {
        for (Method method : timeStepModel.getActiveTimeStepMethods(executionGroup)) {
            Probe probe = binding.getOrCreateProbe(method.getName(), false);
            if (probe != null) {
                probeMap.put(method.getName(), probe);
            }
        }
    }

    public long iteration() {
        return iterations.get();
    }

    @Override
    public final void run() {
        String threadName = Thread.currentThread().getName();
        if (delayMillis > 0) {
            try {
                Thread.sleep(delayMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(
                        threadName + " got interrupted while waiting during rampup.");
            }
        }

        logger.info(threadName + " started");
        try {
            beforeRun();

            boolean explicitStop = false;
            try {
                timeStepLoop();
            } catch (StopException e) {
                explicitStop = true;
                logger.info(threadName + " stopped using StopException");
            }

            afterRun();

            logger.info(threadName + " completed normally" + (explicitStop ? " with StopException" : ""));
        } catch (Throwable e) {
            logger.warn(threadName + " completed with exception " + e.getClass().getName()
                    + " message: " + e.getMessage());
            throw rethrow(e);
        }
    }

    private Object initThreadState() {
        Constructor constructor = timeStepModel.getThreadStateConstructor(executionGroup);
        if (constructor == null) {
            return null;
        }

        Object[] args = constructor.getParameterTypes().length == 0
                ? new Object[]{}
                : new Object[]{testInstance};

        try {
            return constructor.newInstance(args);
        } catch (Exception e) {
            throw new IllegalTestException(
                    format("Failed to create an instance of thread state class '%s'",
                            timeStepModel.getThreadStateClass(executionGroup).getName()), e);
        }
    }

    private void beforeRun() throws Exception {
        for (Method beforeRunMethod : timeStepModel.getBeforeRunMethods(executionGroup)) {
            run(beforeRunMethod);
        }
    }

    protected abstract void timeStepLoop() throws Exception;

    private void afterRun() throws Exception {
        for (Method afterRunMethod : timeStepModel.getAfterRunMethods(executionGroup)) {
            run(afterRunMethod);
        }
    }

    private void run(Method method) throws IllegalAccessException, InvocationTargetException {
        int argCount = method.getParameterTypes().length;
        switch (argCount) {
            case 0:
                method.invoke(testInstance);
                break;
            case 1:
                method.invoke(testInstance, threadState);
                break;
            default:
                throw new IllegalTestException("Unhandled number of arguments for '" + method + "'");
        }
    }
}
