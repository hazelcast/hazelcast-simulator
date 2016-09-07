package com.hazelcast.simulator.worker;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.protocol.StubPromise;
import com.hazelcast.simulator.protocol.operation.ExecuteScriptOperation;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class ScriptExecutorTest {

    private HazelcastInstance hazelcastInstance;
    private ScriptExecutor scriptExecutor;

    @Before
    public void setup() {
        hazelcastInstance = mock(HazelcastInstance.class);
        scriptExecutor = new ScriptExecutor(hazelcastInstance);
    }

    @Test
    public void bash() {
        ExecuteScriptOperation scriptOperation = new ExecuteScriptOperation("bash:ls", false);
        StubPromise promise = new StubPromise();
        scriptExecutor.execute(scriptOperation, promise);
        assertEquals(SUCCESS, promise.join());
    }

    @Test
    public void javascript() {
        ExecuteScriptOperation scriptOperation = new ExecuteScriptOperation("js:java.lang.System.out.println();", false);
        StubPromise promise = new StubPromise();
        System.out.println();
        scriptExecutor.execute(scriptOperation, promise);
        assertEquals(SUCCESS, promise.join());
    }

    @Test
    public void whenFireSndForget_thenErrorNotNoticed() {
        ExecuteScriptOperation scriptOperation = new ExecuteScriptOperation("bash:foobar", true);
        StubPromise promise = new StubPromise();

        scriptExecutor.execute(scriptOperation, promise);
        assertEquals(SUCCESS, promise.join());
    }
}

