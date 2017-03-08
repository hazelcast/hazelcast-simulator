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
package com.hazelcast.simulator.utils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

public final class ExecutorFactory {

    private ExecutorFactory() {
    }

    public static ExecutorService createFixedThreadPool(int poolSize, Class classType) {
        return createFixedThreadPool(poolSize, getName(classType));
    }

    public static ExecutorService createFixedThreadPool(int poolSize, String namePrefix) {
        return Executors.newFixedThreadPool(poolSize, createThreadFactory(namePrefix));
    }

    public static ScheduledExecutorService createScheduledThreadPool(int poolSize, String namePrefix) {
        return Executors.newScheduledThreadPool(poolSize, createThreadFactory(namePrefix));
    }

    private static String getName(Class classType) {
        return classType.getSimpleName().toLowerCase();
    }

    private static ThreadFactory createThreadFactory(String namePrefix) {
        return new ThreadFactoryBuilder().setNameFormat(namePrefix + "-thread-%d").build();
    }
}
