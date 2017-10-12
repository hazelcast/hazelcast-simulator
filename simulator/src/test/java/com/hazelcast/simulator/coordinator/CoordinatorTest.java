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
package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.agent.Agent;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.protocol.StubPromise;
import com.hazelcast.simulator.protocol.operation.RcTestRunOperation;
import com.hazelcast.simulator.protocol.operation.RcTestStatusOperation;
import com.hazelcast.simulator.protocol.operation.RcTestStopOperation;
import com.hazelcast.simulator.protocol.operation.RcWorkerKillOperation;
import com.hazelcast.simulator.protocol.operation.RcWorkerScriptOperation;
import com.hazelcast.simulator.protocol.operation.RcWorkerStartOperation;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.protocol.registry.WorkerQuery;
import com.hazelcast.simulator.tests.SuccessTest;
import com.hazelcast.simulator.utils.AssertTask;
import com.hazelcast.simulator.utils.FileUtils;
import com.hazelcast.simulator.utils.SimulatorUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.TestEnvironmentUtils.localResourceDirectory;
import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.TestEnvironmentUtils.tearDownFakeEnvironment;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

public class CoordinatorTest {

    private static Coordinator coordinator;
    private static String hzConfig;
    private static ComponentRegistry componentRegistry;
    private static String hzClientConfig;
    private static AgentData agentData;
    private static Agent agent;

    private int initialWorkerIndex;
    private int initialTestIndex;

    @BeforeClass
    public static void beforeClass() {
        setupFakeEnvironment();

        hzConfig = FileUtils.fileAsText(new File(localResourceDirectory(), "hazelcast.xml"));
        hzClientConfig = FileUtils.fileAsText(new File(localResourceDirectory(), "client-hazelcast.xml"));

        File simulatorPropertiesFile = new File(getUserDir(), "simulator.properties");
        appendText("CLOUD_PROVIDER=embedded\n", simulatorPropertiesFile);
        SimulatorProperties simulatorProperties = SimulatorUtils.loadSimulatorProperties();

        CoordinatorParameters coordinatorParameters = new CoordinatorParameters()
                .setSimulatorProperties(simulatorProperties)
                .setSkipShutdownHook(true);

        agent = new Agent(1, "127.0.0.1", simulatorProperties.getAgentPort(), 10, 60);
        agent.start();
        agent.setSessionId(coordinatorParameters.getSessionId());

        componentRegistry = new ComponentRegistry();
        agentData = componentRegistry.addAgent("127.0.0.1", "127.0.0.1");
        coordinator = new Coordinator(componentRegistry, coordinatorParameters);
        coordinator.start();
    }

    @Before
    public void before() {
        initialWorkerIndex = agentData.getCurrentWorkerIndex();
        initialTestIndex = componentRegistry.getInitialTestIndex();
    }

    @After
    public void after() throws Exception {
        coordinator.workerKill(new RcWorkerKillOperation("js:java.lang.System.exit(0);", new WorkerQuery()));
    }

    @AfterClass
    public static void afterClass() {
        closeQuietly(coordinator);
        closeQuietly(agent);
        tearDownFakeEnvironment();
    }

    private TestSuite newBasicTestSuite() {
        return new TestSuite()
                .setDurationSeconds(5)
                .addTest(new TestCase("foo")
                        .setProperty("threadCount", 1)
                        .setProperty("class", SuccessTest.class));
    }

    private void assertTestCompletesEventually(final String testId) {
        assertTestStateEventually(testId, "completed");
    }

    private void assertTestStateEventually(final String testId, final String expectedState) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                String status = coordinator.testStatus(new RcTestStatusOperation(testId));
                System.out.println("Status: " + status + " expected: " + expectedState);
                assertEquals(expectedState, status);
            }
        });
    }

    @Test
    public void workersStart_multipleWorkers() throws Exception {
        assertEquals("C_A1_W" + (initialWorkerIndex + 1),
                coordinator.workerStart(new RcWorkerStartOperation()
                        .setWorkerType("member").setHzConfig(hzConfig)));
        assertEquals("C_A1_W" + (initialWorkerIndex + 2),
                coordinator.workerStart(new RcWorkerStartOperation()
                        .setWorkerType("member").setHzConfig(hzConfig)));
        assertEquals("C_A1_W" + (initialWorkerIndex + 3),
                coordinator.workerStart(new RcWorkerStartOperation()
                        .setWorkerType("member").setHzConfig(hzConfig)));
    }

    @Test
    public void workerStart_multipleClients() throws Exception {
        assertEquals("C_A1_W" + (initialWorkerIndex + 1),
                coordinator.workerStart(new RcWorkerStartOperation()
                        .setWorkerType("member").setHzConfig(hzConfig)));
        assertEquals("C_A1_W" + (initialWorkerIndex + 2),
                coordinator.workerStart(new RcWorkerStartOperation()
                        .setWorkerType("javaclient").setHzConfig(hzClientConfig)));
        assertEquals("C_A1_W" + (initialWorkerIndex + 3),
                coordinator.workerStart(new RcWorkerStartOperation()
                        .setWorkerType("javaclient").setHzConfig(hzClientConfig)));
    }

    @Test
    public void workerStart_multipleLiteMembers() throws Exception {
        assertEquals("C_A1_W" + (initialWorkerIndex + 1),
                coordinator.workerStart(new RcWorkerStartOperation()
                        .setWorkerType("member").setHzConfig(hzConfig)));
        assertEquals("C_A1_W" + (initialWorkerIndex + 2),
                coordinator.workerStart(new RcWorkerStartOperation()
                        .setWorkerType("litemember").setHzConfig(hzConfig)));
        assertEquals("C_A1_W" + (initialWorkerIndex + 3),
                coordinator.workerStart(new RcWorkerStartOperation()
                        .setWorkerType("litemember").setHzConfig(hzConfig)));
    }

    @Test
    public void testStartTest() throws Exception {
        coordinator.workerStart(new RcWorkerStartOperation()
                .setHzConfig(hzConfig));

        TestSuite suite = newBasicTestSuite()
                .setDurationSeconds(10);

        StubPromise promise = new StubPromise();
        coordinator.testRun(new RcTestRunOperation(suite).setAsync(true), promise);
        assertEquals(SUCCESS, promise.get());

        String testId = promise.getResponse();
        assertEquals("C_A*_W*_T" + (initialTestIndex + 1), testId);

        assertTestCompletesEventually(testId);
    }

    @Test
    public void testStopTest() throws Exception {
        coordinator.workerStart(new RcWorkerStartOperation()
                .setHzConfig(hzConfig));

        TestSuite suite = newBasicTestSuite()
                .setDurationSeconds(0);

        StubPromise promise = new StubPromise();
        coordinator.testRun(new RcTestRunOperation(suite).setAsync(true), promise);

        String testId = promise.getResponse();

        assertTestStateEventually(testId, "run");

        coordinator.testStop(new RcTestStopOperation(testId));

        assertTestCompletesEventually(testId);
    }

    @Test
    public void testRun() throws Exception {
        // start worker
        coordinator.workerStart(new RcWorkerStartOperation().setHzConfig(hzConfig));

        TestSuite suite = newBasicTestSuite();

        StubPromise promise = new StubPromise();
        coordinator.testRun(new RcTestRunOperation(suite).setAsync(false), promise);

        assertEquals(SUCCESS, promise.get());
    }

    @Test
    public void workerScript() throws Exception {
        coordinator.workerStart(new RcWorkerStartOperation().setHzConfig(hzConfig));

        StubPromise promise = new StubPromise();
        coordinator.workerScript(new RcWorkerScriptOperation("js:'a'"), promise);

        assertEquals(SUCCESS, promise.get());
        assertEquals(format("C_A1_W%s=a", (initialWorkerIndex + 1)), promise.getResponse().replace("\n", ""));
    }
}
