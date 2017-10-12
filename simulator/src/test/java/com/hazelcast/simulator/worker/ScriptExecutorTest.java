/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

    private ScriptExecutor scriptExecutor;

    @Before
    public void setup() {
        HazelcastInstance hazelcastInstance = mock(HazelcastInstance.class);
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

