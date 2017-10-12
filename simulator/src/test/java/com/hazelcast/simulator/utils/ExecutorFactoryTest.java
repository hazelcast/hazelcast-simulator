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

import org.junit.After;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.ExecutorFactory.createFixedThreadPool;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ExecutorFactoryTest {

    private ExecutorService executorService;

    @After
    public void after() throws Exception {
        if (executorService != null) {
            executorService.shutdown();
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(ExecutorFactory.class);
    }

    @Test
    public void testFixedThreadPool() throws Exception {
        executorService = createFixedThreadPool(3, ExecutorFactoryTest.class);
        assertNotNull(executorService);

        Future<Boolean> future = executorService.submit(new ExecutorCallable());
        assertTrue(future.get());
    }

    private static final class ExecutorCallable implements Callable<Boolean> {

        @Override
        public Boolean call() {
            return true;
        }
    }
}
