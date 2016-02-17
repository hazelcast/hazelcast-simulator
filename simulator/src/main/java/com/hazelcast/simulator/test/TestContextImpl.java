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

public class TestContextImpl implements TestContext {

    private final HazelcastInstance hazelcastInstance;
    private final String testId;
    private final String publicIpAddress;

    private volatile boolean stopped;

    public TestContextImpl(String testId) {
        this(null, testId, LOCALHOST);
    }

    public TestContextImpl(HazelcastInstance hazelcastInstance, String testId) {
        this(hazelcastInstance, testId, LOCALHOST);
    }

    public TestContextImpl(HazelcastInstance hazelcastInstance, String testId, String publicIpAddress) {
        this.hazelcastInstance = hazelcastInstance;
        this.testId = testId;
        this.publicIpAddress = publicIpAddress;
    }

    @Override
    public HazelcastInstance getTargetInstance() {
        return hazelcastInstance;
    }

    @Override
    public String getTestId() {
        return testId;
    }

    @Override
    public String getPublicIpAddress() {
        return publicIpAddress;
    }

    @Override
    public boolean isStopped() {
        return stopped;
    }

    @Override
    public void stop() {
        stopped = true;
    }
}
