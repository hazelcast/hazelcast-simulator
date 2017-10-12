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
package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.simulator.agent.Agent;
import com.hazelcast.simulator.agent.workerprocess.WorkerProcess;
import com.hazelcast.simulator.agent.workerprocess.WorkerProcessFailureMonitor;
import com.hazelcast.simulator.agent.workerprocess.WorkerProcessManager;
import com.hazelcast.simulator.agent.workerprocess.WorkerProcessSettings;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.WorkerType;
import com.hazelcast.simulator.protocol.connector.AgentConnector;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.operation.CreateTestOperation;
import com.hazelcast.simulator.protocol.operation.CreateWorkerOperation;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.operation.StartTimeoutDetectionOperation;
import com.hazelcast.simulator.protocol.operation.StopTimeoutDetectionOperation;
import com.hazelcast.util.EmptyStatement;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.TestEnvironmentUtils.tearDownFakeEnvironment;
import static com.hazelcast.simulator.protocol.core.ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.protocol.operation.OperationType.getOperationType;
import static com.hazelcast.simulator.protocol.processors.OperationTestUtil.processOperation;
import static com.hazelcast.simulator.utils.ExecutorFactory.createScheduledThreadPool;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.NativeUtils.execute;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AgentOperationProcessorTest {

    private static final int DEFAULT_TEST_TIMEOUT = 30000;
    private static final int DEFAULT_STARTUP_TIMEOUT = 10;

    private final WorkerProcessFailureMonitor failureMonitor = mock(WorkerProcessFailureMonitor.class);
    private final WorkerProcessManager workerProcessManager = new WorkerProcessManager();
    private final ScheduledExecutorService scheduler = createScheduledThreadPool(3, "AgentOperationProcessorTest");

    private File sessionDir;

    private AgentOperationProcessor processor;

    @Before
    public void before() {
        setupFakeEnvironment();

        File workersDir = new File(getSimulatorHome(), "workers");
        sessionDir = new File(workersDir, "AgentOperationProcessorTest").getAbsoluteFile();

        AgentConnector agentConnector = mock(AgentConnector.class);

        Agent agent = mock(Agent.class);
        when(agent.getAddressIndex()).thenReturn(1);
        when(agent.getPublicAddress()).thenReturn("127.0.0.1");
        when(agent.getSessionId()).thenReturn("AgentOperationProcessorTest");
        when(agent.getSessionDirectory()).thenReturn(sessionDir);
        when(agent.getAgentConnector()).thenReturn(agentConnector);
        when(agent.getWorkerProcessFailureMonitor()).thenReturn(failureMonitor);

        processor = new AgentOperationProcessor(agent, workerProcessManager, scheduler);
    }

    @After
    public void after() throws Exception {
        tearDownFakeEnvironment();

        scheduler.shutdown();
        scheduler.awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test
    public void testProcessOperation_unsupportedOperation() throws Exception {
        SimulatorOperation operation = new CreateTestOperation(1, new TestCase("AgentOperationProcessorTest"));
        ResponseType responseType = processOperation(processor, getOperationType(operation), operation, COORDINATOR);

        assertEquals(UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR, responseType);
    }

    @Test
    public void process_IntegrationTestOperation_unsupportedOperation() throws Exception {
        SimulatorOperation operation = new IntegrationTestOperation();
        ResponseType responseType = processOperation(processor, getOperationType(operation), operation, COORDINATOR);

        assertEquals(UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR, responseType);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testCreateWorkerOperation() throws Exception {
        ResponseType responseType = testCreateWorkerOperation(false, DEFAULT_STARTUP_TIMEOUT);
        assertEquals(SUCCESS, responseType);
        assertWorkerLifecycle();
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testCreateWorkerOperation_withUploadDirectory() throws Exception {
        File uploadDir = ensureExistingDirectory(sessionDir, "upload");
        ensureExistingFile(uploadDir, "testFile");

        ResponseType responseType = testCreateWorkerOperation(false, DEFAULT_STARTUP_TIMEOUT);
        assertEquals(SUCCESS, responseType);

        assertThatFileExistsInWorkerHomes("testFile");
        assertWorkerLifecycle();
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testCreateWorkerOperation_withoutStartupTimeout() throws Exception {
        ResponseType responseType = testCreateWorkerOperation(false, 0);
        assertEquals(EXCEPTION_DURING_OPERATION_EXECUTION, responseType);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testCreateWorkerOperation_withStartupException() throws Exception {
        ResponseType responseType = testCreateWorkerOperation(true, DEFAULT_STARTUP_TIMEOUT);
        assertEquals(EXCEPTION_DURING_OPERATION_EXECUTION, responseType);
    }

    @Test
    public void testStartTimeoutDetectionOperation() throws Exception {
        SimulatorOperation operation = new StartTimeoutDetectionOperation();
        ResponseType responseType = processOperation(processor, getOperationType(operation), operation, COORDINATOR);

        assertEquals(SUCCESS, responseType);

        verify(failureMonitor).startTimeoutDetection();
    }

    @Test
    public void testStopTimeoutDetectionOperation() throws Exception {
        SimulatorOperation operation = new StopTimeoutDetectionOperation();
        ResponseType responseType =processOperation(processor, getOperationType(operation), operation, COORDINATOR);

        assertEquals(SUCCESS, responseType);

        verify(failureMonitor).stopTimeoutDetection();
    }

    private ResponseType testCreateWorkerOperation(boolean withStartupException, int startupTimeout) throws Exception {
        WorkerProcessSettings workerProcessSettings = new WorkerProcessSettings(
                23,
                WorkerType.MEMBER,
                "bringmyown",
                withStartupException ? "exit 1" : "echo $$ > worker.pid; echo '127.0.0.1:5701' > worker.address; sleep 5;",
                startupTimeout,
                new HashMap<String, String>());

        SimulatorOperation operation = new CreateWorkerOperation(singletonList(workerProcessSettings), 0);
        return processOperation(processor, getOperationType(operation), operation, COORDINATOR);
    }

    private void assertWorkerLifecycle() throws InterruptedException {
        for (WorkerProcess workerProcess : workerProcessManager.getWorkerProcesses()) {
            File workerDir = new File(sessionDir, workerProcess.getId());
            assertTrue(workerDir.exists());

            try {
                workerProcess.getProcess().exitValue();
                fail("Expected IllegalThreadStateException since process should still be alive!");
            } catch (IllegalThreadStateException e) {
                EmptyStatement.ignore(e);
            }

            File pidFile = new File(workerDir, "worker.pid");
            String pid = fileAsText(pidFile);
            execute("kill " + pid);

            workerProcess.getProcess().waitFor();
            workerProcess.getProcess().exitValue();

            deleteQuiet(pidFile);
        }
    }

    private void assertThatFileExistsInWorkerHomes(String fileName) {
        for (WorkerProcess workerProcess : workerProcessManager.getWorkerProcesses()) {
            File workerHome = new File(sessionDir, workerProcess.getId()).getAbsoluteFile();
            assertTrue(format("WorkerHome %s should exist", workerHome), workerHome.exists());

            File file = new File(workerHome, fileName);
            assertTrue(format("File %s should exist in %s", fileName, workerHome), file.exists());
        }
    }
}
