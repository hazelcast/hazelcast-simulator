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
package com.hazelcast.simulator.worker;

import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.IllegalTestException;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.InjectMetronome;
import com.hazelcast.simulator.test.annotations.InjectProbe;
import com.hazelcast.simulator.test.annotations.InjectTestContext;
import com.hazelcast.simulator.worker.metronome.Metronome;
import com.hazelcast.simulator.worker.tasks.IWorker;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.utils.CommonUtils.rethrow;

public abstract class TimeStepTask implements Runnable {
    @InjectTestContext
    protected TestContext testContext;
    @InjectMetronome
    protected Metronome metronome;
    @InjectProbe(name = IWorker.DEFAULT_WORKER_PROBE_NAME, useForThroughput = true)
    protected Probe workerProbe;

    protected final Object threadContext;
    protected final Object testInstance;
    protected final AtomicLong iterations = new AtomicLong();
    protected final TimeStepModel timeStepModel;
    protected final byte[] timeStepProbabilities;

    public TimeStepTask(Object testInstance, TimeStepModel timeStepModel) {
        this.testInstance = testInstance;
        this.timeStepModel = timeStepModel;
        this.threadContext = initThreadContext();
        this.timeStepProbabilities = timeStepModel.getTimeStepProbabilityArray();
    }

    public void setWorkerProbe(Probe workerProbe) {
        this.workerProbe = workerProbe;
    }

    public long iteration() {
        return iterations.get();
    }

    @Override
    public final void run() {
        try {
            beforeRun();
            timeStepLoop();
            afterRun();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private Object initThreadContext() {
        Class workerContextClazz = timeStepModel.getThreadContextClass();
        if (workerContextClazz == null) {
            return null;
        }

        Constructor constructor = null;
        Object[] args = null;
        try {
            constructor = workerContextClazz.getDeclaredConstructor();
            args = new Object[0];
        } catch (NoSuchMethodException ignore) {
            //todo: remove
            ignore.printStackTrace();
        }

        try {
            constructor = workerContextClazz.getDeclaredConstructor(testInstance.getClass());
            args = new Object[]{testInstance};
        } catch (NoSuchMethodException ignore) {
            //todo: remove
            ignore.printStackTrace();
        }

        if (constructor == null) {
            throw new IllegalTestException("No valid constructor found for " + workerContextClazz.getName());
        }

        try {
            constructor.setAccessible(true);
            return constructor.newInstance((Object[]) args);
        } catch (IllegalAccessException e) {
            throw new IllegalTestException(e.getMessage(), e);
        } catch (InstantiationException e) {
            throw new IllegalTestException(e.getMessage(), e);
        } catch (InvocationTargetException e) {
            throw new IllegalTestException(e.getMessage(), e);
        }
    }

    private void beforeRun() throws Exception {
        for (Method beforeRunMethod : timeStepModel.getBeforeRunMethods()) {
            int argCount = beforeRunMethod.getParameterTypes().length;
            switch (argCount) {
                case 0:
                    beforeRunMethod.invoke(testInstance);
                    break;
                case 1:
                    beforeRunMethod.invoke(testInstance, threadContext);
                    break;
                default:
                    throw new RuntimeException();
            }
        }
    }

    protected abstract void timeStepLoop() throws Exception;

    private void afterRun() throws Exception {
        for (Method afterRunMethod : timeStepModel.getAfterRunMethods()) {
            int argCount = afterRunMethod.getParameterTypes().length;
            switch (argCount) {
                case 0:
                    afterRunMethod.invoke(testInstance);
                    break;
                case 1:
                    afterRunMethod.invoke(testInstance, threadContext);
                    break;
                default:
                    throw new RuntimeException();
            }
        }
    }
}
