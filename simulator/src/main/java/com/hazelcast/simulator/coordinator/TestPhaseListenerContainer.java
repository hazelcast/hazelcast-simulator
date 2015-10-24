/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.test.TestPhase;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TestPhaseListenerContainer {

    private final ConcurrentMap<String, TestPhaseListener> testCaseRunnerMap = new ConcurrentHashMap<String, TestPhaseListener>();

    public Collection<TestPhaseListener> getListeners() {
        return testCaseRunnerMap.values();
    }

    public void addListener(String testId, TestPhaseListener testCaseRunner) {
        testCaseRunnerMap.put(testId, testCaseRunner);
    }

    public void updatePhaseCompletion(String testId, TestPhase testPhase) {
        TestPhaseListener testCaseRunner = testCaseRunnerMap.get(testId);
        testCaseRunner.completed(testPhase);
    }
}
