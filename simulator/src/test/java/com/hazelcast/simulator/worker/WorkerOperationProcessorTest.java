package com.hazelcast.simulator.worker;


import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.coordinator.operations.FailureOperation;
import com.hazelcast.simulator.protocol.Promise;
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
import org.mockito.Matchers;
import org.mockito.Mockito;

import javax.jms.IllegalStateException;
import java.io.File;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.teardownFakeUserDir;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class WorkerOperationProcessorTest {

    private WorkerOperationProcessor processor;
    private TestManager testManager;
    private Worker worker;
    private SimulatorAddress sourceAddress;
    private Promise promise;
    private ScriptExecutor scriptExecutor;

    @Before
    public void before() {
        setupFakeUserDir();
        ExceptionReporter.reset();
        testManager = mock(TestManager.class);
        worker = mock(Worker.class);
        scriptExecutor = mock(ScriptExecutor.class);
        processor = new WorkerOperationProcessor(worker, testManager, scriptExecutor);
        sourceAddress = COORDINATOR;
        promise = mock(Promise.class);
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
        verify(promise).answer(Matchers.anyObject());
    }

    @Test
    public void test_CreateTestOperation() throws Exception {
        CreateTestOperation op = new CreateTestOperation(new TestCase("foo"));

        processor.process(op, sourceAddress, promise);

        verify(testManager).createTest(op);
        verify(promise).answer(Matchers.anyObject());
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
        verify(promise).answer(Matchers.anyObject());
    }

    // make sure that unhandled exceptions are trapped.
    @Test
    public void test_unhandledException() throws Exception {
        StopRunOperation op = new StopRunOperation("foo");
        Exception e = new IndexOutOfBoundsException("");
        doThrow(e).when(testManager).stopRun(op);

        try {
            processor.process(op, sourceAddress, promise);
            fail();
        } catch (IndexOutOfBoundsException found) {
        }

        File exceptionFile = new File(getUserDir(), "1.exception");
        assertTrue(exceptionFile.exists());
        assertFalse(new File(getUserDir(), "1.exception.tmp").exists());
        assertNotNull(fileAsText(exceptionFile));
    }

    @Test(expected = ProcessException.class)
    public void testUnhandledOperation() throws Exception {
        processor.process(mock(FailureOperation.class), sourceAddress, promise);
    }
}
