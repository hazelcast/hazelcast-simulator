package com.hazelcast.simulator.agent;

import com.hazelcast.simulator.test.TestSuite;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class AgentCliTest {

    private final List<String> args = new ArrayList<String>();

    private Agent agent;

    @After
    public void tearDown() throws Exception {
        if (agent != null) {
            agent.shutdown();
        }
        deleteQuiet(new File("./logs"));
    }

    @Test
    public void testInit() {
        args.add("--addressIndex");
        args.add("5");
        args.add("--publicAddress");
        args.add("127.0.0.1");

        agent = createAgent();

        assertEquals(5, agent.getAddressIndex());
        assertEquals("127.0.0.1", agent.getPublicAddress());
        assertNull(agent.getTestSuiteDir());

        TestSuite testSuite = new TestSuite();
        agent.setTestSuite(testSuite);
        assertNotNull(agent.getTestSuiteDir());
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_missingAddressIndex() {
        args.add("--publicAddress");
        args.add("127.0.0.1");

        createAgent();
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_missingPublicAddress() {
        args.add("--addressIndex");
        args.add("1");

        createAgent();
    }

    private Agent createAgent() {
        return AgentCli.init(getArgs(true));
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
