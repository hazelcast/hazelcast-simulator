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
package com.hazelcast.simulator.test;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.test.annotations.InjectTestContext;
import com.hazelcast.simulator.test.annotations.InjectVendor;
import org.apache.log4j.Logger;

/**
 * An Abstract Test that provides basic behavior so it doesn't need to be repeated for every test.
 *
 * Coordinator doesn't not impose the AbstractTest to be the part of any test class. So if you want
 * to use it, use it; otherwise you can define your own hierarchy.
 *
 * @deprecated since 0.10. Use com.hazelcast.simulator.hz.HazelcastTest instead.
 */
public abstract class AbstractTest {

    /**
     * Returns the name of the configured data-structure. Normally your tests will contain a 'name'
     * property so you can define e.g. 'offheapMap' or 'onheapMap' etc. This way you can change the
     * behavior of the test by switching to a different data-structure.
     */
    @SuppressWarnings("checkstyle:visibilitymodifier")
    public String name = getClass().getSimpleName();

    protected final Logger logger = Logger.getLogger(getClass());

    @InjectVendor
    protected HazelcastInstance targetInstance;

    @InjectTestContext
    protected TestContext testContext;
}
