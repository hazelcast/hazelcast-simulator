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
package com.hazelcast.simulator;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestSupport {

    public static <E> Future<E> spawn(Callable<E> e) {
        FutureTask<E> task = new FutureTask<E>(e);
        Thread thread = new Thread(task);
        thread.start();
        return task;
    }

    @SuppressWarnings("unchecked")
    public static <E> E assertInstanceOf(Class<E> clazz, Object object) {
        assertNotNull(object);
        assertTrue(object + " is not an instanceof " + clazz.getName(), clazz.isAssignableFrom(object.getClass()));
        return (E) object;
    }

    public static Map<String, String> toMap(String... array) {
        if (array.length % 2 == 1) {
            throw new IllegalArgumentException("even number of arguments expected");
        }

        Map<String, String> result = new HashMap<String, String>();
        for (int k = 0; k < array.length; k += 2) {
            result.put(array[k], array[k + 1]);
        }
        return result;
    }
}
