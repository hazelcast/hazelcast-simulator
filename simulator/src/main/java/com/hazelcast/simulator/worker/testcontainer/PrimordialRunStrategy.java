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

import java.lang.reflect.Method;
import java.util.concurrent.Callable;

/**
 * {@link RunStrategy} uses for a test with a method annotated with {@link com.hazelcast.simulator.test.annotations.Run}.
 */
public class PrimordialRunStrategy extends RunStrategy {

    private final Object instance;
    private final Method method;
    private final Object[] args;

    public PrimordialRunStrategy(Object instance, Method method, Object... args) {
        this.instance = instance;
        this.method = method;
        this.args = args;
    }

    @Override
    public Callable getRunCallable() {
        return () -> {
            onRunStarted();
            try {
                return method.invoke(instance, args);
            } finally {
                onRunCompleted();
            }
        };
    }
}
