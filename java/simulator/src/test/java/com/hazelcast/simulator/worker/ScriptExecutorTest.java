package com.hazelcast.simulator.worker;

import com.hazelcast.simulator.protocol.StubPromise;
import com.hazelcast.simulator.drivers.Driver;
import com.hazelcast.simulator.worker.messages.ExecuteScriptMessage;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class ScriptExecutorTest {

    private Driver driver;
    private ScriptExecutor scriptExecutor;

    @Before
    public void setup() {
        driver = mock(Driver.class);
        scriptExecutor = new ScriptExecutor(driver);
    }

    @Test
    public void bash() {
        ExecuteScriptMessage scriptOperation = new ExecuteScriptMessage("bash:ls", false);
        StubPromise promise = new StubPromise();
        scriptExecutor.execute(scriptOperation, promise);

        promise.assertCompletesEventually();
        assertTrue(promise.getAnswer() instanceof String);
    }

    @Test
    public void javascript() {
        ExecuteScriptMessage scriptOperation = new ExecuteScriptMessage("js:java.lang.System.out.println();", false);
        StubPromise promise = new StubPromise();

        scriptExecutor.execute(scriptOperation, promise);

        promise.assertCompletesEventually();
        assertTrue(promise.getAnswer() instanceof String);
    }

    @Test
    public void whenFireSndForget_thenErrorNotNoticed() {
        ExecuteScriptMessage scriptOperation = new ExecuteScriptMessage("bash:foobar", true);

        StubPromise promise = new StubPromise();

        scriptExecutor.execute(scriptOperation, promise);
        promise.assertCompletesEventually();
        assertTrue(promise.getAnswer() instanceof String);
    }
}

