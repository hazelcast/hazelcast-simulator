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
package com.hazelcast.simulator.coordinator.registry;

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.coordinator.TestSuite;

import static com.hazelcast.simulator.coordinator.registry.TestData.CompletedStatus.FAILED;
import static com.hazelcast.simulator.coordinator.registry.TestData.CompletedStatus.IN_PROGRESS;
import static com.hazelcast.simulator.coordinator.registry.TestData.CompletedStatus.SUCCESS;

public class TestData {

    public enum CompletedStatus {
        IN_PROGRESS,
        FAILED,
        SUCCESS
    }

    private final int testIndex;
    private final TestCase testCase;
    private final TestSuite testSuite;
    private volatile long startTimeMillis;
    private volatile TestPhase testPhase;
    private volatile boolean stopRequested;
    private volatile CompletedStatus completedStatus = IN_PROGRESS;

    TestData(int testIndex,  TestCase testCase, TestSuite testSuite) {
        this.testIndex = testIndex;
        this.testCase = testCase;
        this.testSuite = testSuite;
    }

    public CompletedStatus getCompletedStatus() {
        return completedStatus;
    }

    public boolean isCompleted() {
        return completedStatus == FAILED || completedStatus == SUCCESS;
    }

    public void setCompletedStatus(CompletedStatus completedStatus) {
        this.completedStatus = completedStatus;
    }

    public void setStopRequested(boolean stopRequested) {
        this.stopRequested = stopRequested;
    }

    public boolean isStopRequested() {
        return stopRequested;
    }

    public void setTestPhase(TestPhase testPhase) {
        this.testPhase = testPhase;
    }

    public int getTestIndex() {
        return testIndex;
    }

    public TestCase getTestCase() {
        return testCase;
    }

    public TestSuite getTestSuite() {
        return testSuite;
    }

    public void initStartTime() {
        this.startTimeMillis = System.currentTimeMillis();
    }

    public long getStartTimeMillis() {
        return startTimeMillis;
    }

    public String getStatusString() {
        TestData.CompletedStatus status = getCompletedStatus();
        if (status == IN_PROGRESS) {
            return testPhase.desc();
        } else {
            return completedStatus == CompletedStatus.SUCCESS ? "completed" : "failed";
        }
    }
}
