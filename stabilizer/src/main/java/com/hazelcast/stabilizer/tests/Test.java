/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.stabilizer.tests;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.stabilizer.performance.Performance;

import java.lang.reflect.Field;
import java.util.Map;

import static java.lang.String.format;

/**
 * The Test is the 'thing' that contains the actual logic we want to run.
 * information.
 * <p/>
 * Order of lifecycle methods:
 * <ol>
 * <li>{@link #localSetup()}</li>
 * <li>{@link #globalSetup()}</li>
 * <li>{@link #start()}</li>
 * <li>{@link #stop(long)}</li>
 * <li>{@link #localVerify()}</li>
 * <li>{@link #globalVerify()}</li>
 * <li>{@link #globalTearDown()}</li>
 * <li>{@link #localTearDown()}</li>
 * </ol>
 */
public interface Test {

    String TEST_INSTANCE = "testInstance";

    /**
     * Sets up this Test
     * <p/>
     * This method will only be called on a single members of the cluster.
     *
     * @throws Exception
     */
    void globalSetup() throws Exception;

    /**
     * Sets up this Test
     * <p/>
     * This method will be called on a all members of the cluster.
     *
     * @throws Exception
     */
    void localSetup() throws Exception;

    /**
     * Tears down this Test
     * <p/>
     * This method will  be called on a all members of the cluster.
     *
     * @throws Exception
     */
    void localTearDown() throws Exception;

    /**
     * Tears down this Test
     * <p/>
     * This method will only be called on a single member of the cluster.
     *
     * @throws Exception
     */
    void globalTearDown() throws Exception;

    void start() throws Exception;

    /**
     * Stops this Test.
     * <p/>
     * This method is synchronous, so after this method completes, the Test really should be stopped.
     *
     * @throws Exception
     */
    void stop(long timeoutMs) throws Exception;

    void localVerify() throws Exception;

    void globalVerify() throws Exception;

    void setHazelcastInstance(HazelcastInstance hazelcastInstance);

    void setTestId(String id);

    Performance calcPerformance();


}
