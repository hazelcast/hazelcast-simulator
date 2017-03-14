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
package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.protocol.Promise;
import com.hazelcast.simulator.protocol.Server;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.worker.operations.CreateTestOperation;
import com.hazelcast.simulator.worker.operations.StartPhaseOperation;
import com.hazelcast.simulator.worker.operations.StopRunOperation;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.simulator.common.TestPhase.getLastTestPhase;
import static java.lang.String.format;

/**
 * Responsible for managing the TestContainers.
 *
 * So creating them, starting their phases, and termination.
 */
public class TestManager {

    private static final Logger LOGGER = Logger.getLogger(TestManager.class);
    private static final String DASHES = "---------------------------";

    private final ConcurrentMap<String, TestContainer> tests = new ConcurrentHashMap<String, TestContainer>();
    private final Server server;
    private final Object vendorInstance;

    public TestManager(Server server, Object vendorInstance) {
        this.server = server;
        this.vendorInstance = vendorInstance;
    }

    public Collection<TestContainer> getContainers() {
        return new ArrayList<TestContainer>(tests.values());
    }

    public void createTest(CreateTestOperation operation) {
        TestCase testCase = operation.getTestCase();

        String testId = testCase.getId();

        TestContainer testContainer = tests.get(testId);

        if (testContainer != null) {
            throw new IllegalStateException(format("Can't init TestCase: %s, another test with testId %s already exists",
                    operation, testId));
        }

        LOGGER.info(format("%s Initializing test %s %s%n%s", DASHES, testId, DASHES, testCase));

        TestContextImpl testContext = new TestContextImpl(testId, null, server);

        testContainer = new TestContainer(testContext, testCase, vendorInstance);

        tests.put(testId, testContainer);
    }

    public void stopRun(StopRunOperation op) {
        String testId = op.getTestId();

        LOGGER.info(format("%s Stopping %s %s", DASHES, testId, DASHES));

        TestContainer testContainer = tests.get(testId);
        if (testContainer == null) {
            throw new IllegalArgumentException(format("Could not stop test, test [%s] is not found.", testId));
        }

        testContainer.getTestContext().stop();
    }

    public void startTestPhase(StartPhaseOperation op, Promise promise) throws Exception {
        TestPhase testPhase = op.getTestPhase();

        String testId = op.getTestId();
        TestContainer testContainer = tests.get(testId);
        if (testContainer == null) {
            throw new IllegalArgumentException(format("Could not start phase [%s] , test [%s] is not found.", testPhase, testId));
        }

        new TestPhaseThread(testContainer, testPhase, testId, promise).start();
    }

    private class TestPhaseThread extends Thread {

        private final TestPhase testPhase;
        private final String testId;
        private final Promise promise;
        private final TestContainer testContainer;

        TestPhaseThread(TestContainer testContainer, TestPhase testPhase, String testId, Promise promise) {
            this.testContainer = testContainer;
            this.testId = testId;
            this.testPhase = testPhase;
            this.promise = promise;
        }

        @Override
        @SuppressWarnings("PMD.AvoidCatchingThrowable")
        public final void run() {
            LOGGER.info(format("%s Starting %s of %s %s", DASHES, testPhase.desc(), testId, DASHES));
            try {
                testContainer.invoke(testPhase);
                LOGGER.info(format("%s %s of %s SUCCEEDED %s ", DASHES, testPhase.desc(), testId, DASHES));
                promise.answer("ok");
            } catch (Throwable t) {
                promise.answer(t);
                LOGGER.error(format("%s %s of %s FAILED %s ", DASHES, testPhase.desc(), testId, DASHES), t);
                ExceptionReporter.report(testId, t);
            } finally {
                if (testPhase == getLastTestPhase()) {
                    tests.remove(testId);
                }
            }
        }
    }
}
