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
package com.hazelcast.simulator.agent;

import com.hazelcast.simulator.utils.CommandLineExitException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.TestEnvironmentUtils.tearDownFakeEnvironment;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class AgentCliTest {

    private final List<String> args = new ArrayList<String>();

    private Agent agent;

    @Before
    public void before() {
        setupFakeEnvironment();
    }

    @After
    public void after() {
        if (agent != null) {
            agent.close();
        }

        tearDownFakeEnvironment();
    }

    @Test
    public void testInit() {
        args.add("--addressIndex");
        args.add("5");
        args.add("--publicAddress");
        args.add("127.0.0.1");
        args.add("--port");
        args.add("9000");

        startAgent();

        assertEquals(5, agent.getAddressIndex());
        assertEquals("127.0.0.1", agent.getPublicAddress());
        try {
            assertNull(agent.getSessionDirectory());
            fail();
        } catch (IllegalStateException ignore) {
        }

        agent.setSessionId("AgentCliTest");
        assertNotNull(agent.getSessionDirectory());
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_missingAddressIndex() {
        args.add("--publicAddress");
        args.add("127.0.0.1");
        args.add("--port");
        args.add("9000");

        startAgent();
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_missingPublicAddress() {
        args.add("--addressIndex");
        args.add("1");
        args.add("--port");
        args.add("9000");

        startAgent();
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_missingPort() {
        args.add("--addressIndex");
        args.add("1");
        args.add("--publicAddress");
        args.add("127.0.0.1");

        startAgent();
    }

    private void startAgent() {
        AgentCli cli = new AgentCli(getArgs(true));
        agent = cli.agent;
        agent.start();
    }

    @SuppressWarnings("SameParameterValue")
    private String[] getArgs(boolean addDefaults) {
        if (addDefaults) {
            args.add("test.properties");
        }

        String[] argsArray = new String[args.size()];
        args.toArray(argsArray);
        return argsArray;
    }
}
