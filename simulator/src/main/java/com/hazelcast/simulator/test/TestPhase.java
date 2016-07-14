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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public enum TestPhase {

    SETUP("setup",  false),
    LOCAL_WARMUP("local warmup", false),
    GLOBAL_WARMUP("global warmup", true),
    RUN("run", false),
    WARMUP("warmup", false),
    LOCAL_RESET("local reset", false),
    GLOBAL_RESET("global reset", true),
    GLOBAL_VERIFY("global verify", true),
    LOCAL_VERIFY("local verify", false),
    GLOBAL_TEARDOWN("global tear down", true),
    LOCAL_TEARDOWN("local tear down", false);

    private static final TestPhase LAST_TEST_PHASE = values()[values().length - 1];

    private final String description;
    private final boolean isGlobal;

    TestPhase(String description, boolean isGlobal) {
        this.description = description;
        this.isGlobal = isGlobal;
    }

    public String desc() {
        return description;
    }

    public boolean isGlobal() {
        return isGlobal;
    }

    public static TestPhase getLastTestPhase() {
        return LAST_TEST_PHASE;
    }

    public static String getIdsAsString() {
        StringBuilder builder = new StringBuilder();
        String delimiter = "";
        for (TestPhase testPhase : values()) {
            builder.append(delimiter).append(testPhase);
            delimiter = ", ";
        }
        return builder.toString();
    }

    public static Map<TestPhase, CountDownLatch> getTestPhaseSyncMap(int testCount, boolean isParallel,
                                                                     TestPhase latestTestPhaseToSync) {
        if (!isParallel) {
            return null;
        }
        Map<TestPhase, CountDownLatch> testPhaseSyncMap = new ConcurrentHashMap<TestPhase, CountDownLatch>();
        boolean setTestCount = true;
        for (TestPhase testPhase : TestPhase.values()) {
            testPhaseSyncMap.put(testPhase, new CountDownLatch(setTestCount ? testCount : 0));
            if (testPhase.equals(latestTestPhaseToSync)) {
                setTestCount = false;
            }
        }
        return testPhaseSyncMap;
    }
}
