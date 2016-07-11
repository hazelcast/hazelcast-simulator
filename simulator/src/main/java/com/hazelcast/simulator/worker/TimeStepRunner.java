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
import com.hazelcast.simulator.test.DependencyInjector;
import com.hazelcast.simulator.test.DependencyInjectorAware;
import com.hazelcast.simulator.test.IllegalTestException;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.InjectMetronome;
import com.hazelcast.simulator.test.annotations.InjectTestContext;
import com.hazelcast.simulator.worker.metronome.Metronome;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.utils.CommonUtils.rethrow;

public abstract class TimeStepRunner implements Runnable, DependencyInjectorAware {
    @InjectTestContext
    protected TestContext testContext;
    @InjectMetronome
    protected Metronome metronome;

    protected final Object threadContext;
    protected final Object testInstance;
    protected final AtomicLong iterations = new AtomicLong();
    protected final TimeStepModel timeStepModel;
    protected final byte[] timeStepProbabilities;
    protected final Map<String, Probe> probeMap = new HashMap<String, Probe>();

    public TimeStepRunner(Object testInstance, TimeStepModel timeStepModel) {
        this.testInstance = testInstance;
        this.timeStepModel = timeStepModel;
        this.threadContext = initThreadContext();
        this.timeStepProbabilities = timeStepModel.getTimeStepProbabilityArray();
    }

    @Override
    public void inject(DependencyInjector injector) {
        for (Method method : timeStepModel.getActiveTimeStepMethods()) {
            Probe probe = injector.getOrCreateProbe(method.getName(), false);
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
        try {
            beforeRun();
            timeStepLoop();
            afterRun();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private Object initThreadContext() {
        Constructor constructor = timeStepModel.getThreadContextConstructor();
        if (constructor == null) {
            return null;
        }

        Object[] args = constructor.getParameterTypes().length == 0
                ? new Object[]{}
                : new Object[]{threadContext};

        try {
            return constructor.newInstance((Object[]) args);
        } catch (Exception e) {
            throw new IllegalTestException(e.getMessage(), e);
        }
    }

    private void beforeRun() throws Exception {
        for (Method beforeRunMethod : timeStepModel.getBeforeRunMethods()) {
            run(beforeRunMethod);
        }
    }

    protected abstract void timeStepLoop() throws Exception;

    private void afterRun() throws Exception {
        for (Method afterRunMethod : timeStepModel.getAfterRunMethods()) {
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
                method.invoke(testInstance, threadContext);
                break;
            default:
                throw new RuntimeException();
        }
    }
}
