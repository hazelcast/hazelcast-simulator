/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.test;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.annotations.InjectHazelcastInstance;
import com.hazelcast.simulator.test.annotations.InjectTestContext;

/**
 * An Abstract Test that provides basic behavior so it doesn't need to be repeated for every test.
 *
 * Coordinator will not impose the AbstractTest to be the part of any test class.
 */
public abstract class AbstractTest {

    @SuppressWarnings("checkstyle:visibilitymodifier")
    public String name = getClass().getSimpleName();

    protected final ILogger logger = Logger.getLogger(getClass());

    @InjectHazelcastInstance
    protected HazelcastInstance targetInstance;

    @InjectTestContext
    protected TestContext testContext;
}
