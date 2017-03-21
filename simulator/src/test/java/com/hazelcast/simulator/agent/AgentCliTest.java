package com.hazelcast.simulator.agent;

import com.hazelcast.simulator.utils.CommandLineExitException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.TestEnvironmentUtils.tearDownFakeEnvironment;
import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
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
        closeQuietly(agent);
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

        assertEquals(5, agent.getProcessManager().getAgentAddress().getAgentIndex());
        assertEquals("127.0.0.1", agent.getPublicAddress());
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

    private String[] getArgs(boolean addDefaults) {
        if (addDefaults) {
            args.add("test.properties");
        }

        String[] argsArray = new String[args.size()];
        args.toArray(argsArray);
        return argsArray;
    }
}
