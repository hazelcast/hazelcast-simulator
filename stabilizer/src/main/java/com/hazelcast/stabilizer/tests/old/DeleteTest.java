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
package com.hazelcast.stabilizer.tests.old;

import com.hazelcast.stabilizer.tests.TestDependencies;

/**
 * The Test is the 'thing' that contains the actual logic we want to run.
 * information.
 * <p/>
 * Order of lifecycle methods:
 * <ol>
 * <li>{@link #setup()}</li>
 * <li>{@link #globalSetup()}</li>
 * <li>{@link #start()}</li>
 * <li>{@link #stopTest(long)}</li>
 * <li>{@link #localVerify()}</li>
 * <li>{@link #globalVerify()}</li>
 * <li>{@link #globalTearDown()}</li>
 * <li>{@link #teardown()}</li>
 * </ol>
 */
public interface DeleteTest {

    void init(TestDependencies dependencies);

    /**
     * Sets up this Test. This is where you set up your local data-structures.
     * <p/>
     * This method will be called on a all members of the cluster.
     * <p/>
     * This method should not be used to warmup the data-structures, e.g. filling them with large
     * amounts of content. We have the warmup for that.
     *
     * @throws Exception
     */
    void setup() throws Exception;

    void warmup() throws Exception;

    /**
     * Starts this test.
     *
     * @param active
     * @throws Exception
     */
    void test(boolean active) throws Exception;

    void stop();

    /**
     * Tears down this Test
     * <p/>
     * This method will  be called on a all members of the cluster.
     *
     * @throws Exception
     */
    void teardown() throws Exception;

    void verify() throws Exception;

    long getOperationCount();

    /**
     * Sets up this Test
     * <p/>
     * This method will only be called on a single members of the cluster.
     *
     * @throws Exception
     */
    void globalSetup() throws Exception;

    /**
     * Tears down this Test
     * <p/>
     * This method will only be called on a single member of the cluster.
     *
     * @throws Exception
     */
    void globalTearDown() throws Exception;
}
