package com.hazelcast.simulator.worker;


import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.coordinator.operations.FailureOperation;
import com.hazelcast.simulator.protocol.StubPromise;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.exception.ProcessException;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.worker.operations.CreateTestOperation;
import com.hazelcast.simulator.worker.operations.ExecuteScriptOperation;
import com.hazelcast.simulator.worker.operations.StartPhaseOperation;
import com.hazelcast.simulator.worker.operations.StopRunOperation;
import com.hazelcast.simulator.worker.operations.TerminateWorkerOperation;
import com.hazelcast.simulator.worker.testcontainer.TestManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.teardownFakeUserDir;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class WorkerOperationProcessorTest {

    private WorkerOperationProcessor processor;
    private TestManager testManager;
    private Worker worker;
    private SimulatorAddress sourceAddress = SimulatorAddress.coordinatorAddress();
    private StubPromise promise;
    private ScriptExecutor scriptExecutor;

    @Before
    public void before() {
        setupFakeUserDir();
        ExceptionReporter.reset();
        testManager = mock(TestManager.class);
        worker = mock(Worker.class);
        scriptExecutor = mock(ScriptExecutor.class);
        processor = new WorkerOperationProcessor(worker, testManager, scriptExecutor);
        promise = new StubPromise();
    }

    @After
    public void after() {
        teardownFakeUserDir();
        ExceptionReporter.reset();
    }

    @Test
    public void test_TerminateWorkerOperation() throws Exception {
        TerminateWorkerOperation op = new TerminateWorkerOperation(true);

        processor.process(op, sourceAddress, promise);

        verify(worker).shutdown(op);
        assertTrue(promise.hasAnswer());
    }

    @Test
    public void test_CreateTestOperation() throws Exception {
        CreateTestOperation op = new CreateTestOperation(new TestCase("foo"));

        processor.process(op, sourceAddress, promise);

        verify(testManager).createTest(op);
        assertTrue(promise.hasAnswer());
    }

    @Test
    public void test_ExecuteScriptOperation() throws Exception {
        ExecuteScriptOperation op = new ExecuteScriptOperation("bash:ls", true);

        processor.process(op, sourceAddress, promise);

        verify(scriptExecutor).execute(op, promise);
    }

    @Test
    public void test_StartTestPhaseOperation() throws Exception {
        StartPhaseOperation op = new StartPhaseOperation(TestPhase.GLOBAL_PREPARE, "foo");

        processor.process(op, sourceAddress, promise);

        verify(testManager).startTestPhase(op, promise);
    }

    @Test
    public void test_StopRunOperation() throws Exception {
        StopRunOperation op = new StopRunOperation("foo");

        processor.process(op, sourceAddress, promise);

        verify(testManager).stopRun(op);
        assertTrue(promise.hasAnswer());
    }

    // make sure that unhandled exceptions are trapped.
    @Test
    public void test_unhandledException() throws Exception {
        StopRunOperation op = new StopRunOperation("foo");
        Exception e = new IndexOutOfBoundsException("");
        doThrow(e).when(testManager).stopRun(op);

        processor.process(op, sourceAddress, promise);

        assertTrue(promise.getAnswer() instanceof IndexOutOfBoundsException);
        File exceptionFile = new File(getUserDir(), "1.exception");
        assertTrue(exceptionFile.exists());
        assertFalse(new File(getUserDir(), "1.exception.tmp").exists());
        assertNotNull(fileAsText(exceptionFile));
    }

    @Test
    public void testUnhandledOperation() throws Exception {
        processor.process(mock(FailureOperation.class), sourceAddress, promise);
        assertTrue(promise.getAnswer() instanceof ProcessException);
    }
}
