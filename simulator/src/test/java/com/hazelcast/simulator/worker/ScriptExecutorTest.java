package com.hazelcast.simulator.worker;

import com.hazelcast.simulator.StubPromise;
import com.hazelcast.simulator.vendors.VendorDriver;
import com.hazelcast.simulator.worker.operations.ExecuteScriptOperation;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class ScriptExecutorTest {

    private VendorDriver vendorDriver;
    private ScriptExecutor scriptExecutor;

    @Before
    public void setup() {
        vendorDriver = mock(VendorDriver.class);
        scriptExecutor = new ScriptExecutor(vendorDriver);
    }

    @Test
    public void bash() {
        ExecuteScriptOperation scriptOperation = new ExecuteScriptOperation("bash:ls", false);
        StubPromise promise = new StubPromise();
        scriptExecutor.execute(scriptOperation, promise);

        promise.assertCompletesEventually();
        assertTrue(promise.getAnswer() instanceof String);
    }

    @Test
    public void javascript() {
        ExecuteScriptOperation scriptOperation = new ExecuteScriptOperation("js:java.lang.System.out.println();", false);
        StubPromise promise = new StubPromise();

        scriptExecutor.execute(scriptOperation, promise);

        promise.assertCompletesEventually();
        assertTrue(promise.getAnswer() instanceof String);
    }

    @Test
    public void whenFireSndForget_thenErrorNotNoticed() {
        ExecuteScriptOperation scriptOperation = new ExecuteScriptOperation("bash:foobar", true);

        StubPromise promise = new StubPromise();

        scriptExecutor.execute(scriptOperation, promise);
        promise.assertCompletesEventually();
        assertTrue(promise.getAnswer() instanceof String);
    }
}

