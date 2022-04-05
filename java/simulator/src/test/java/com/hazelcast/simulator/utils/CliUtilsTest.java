package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.utils.helper.ExitExceptionSecurityManager;
import com.hazelcast.simulator.utils.helper.ExitStatusZeroException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.simulator.utils.CliUtils.initOptionsWithHelp;
import static com.hazelcast.simulator.utils.CliUtils.printHelpAndExit;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CliUtilsTest {

    private final OptionParser parser = new OptionParser();

    private SecurityManager oldSecurityManager;

    @Before
    public void before() {
        oldSecurityManager = System.getSecurityManager();
        System.setSecurityManager(new ExitExceptionSecurityManager(true));
    }

    @After
    public void after() {
        System.setSecurityManager(oldSecurityManager);
    }

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(CliUtils.class);
    }

    @Test(expected = ExitStatusZeroException.class)
    public void testInitOptionsWithHelp_help() {
        initOptionsWithHelp(parser, new String[]{"--help"});
    }

    @Test
    public void testInitOptionsWithHelp_noArgs() {
        OptionSet options = initOptionsWithHelp(parser, new String[]{});
        assertNotNull(options);
        assertEquals(0, options.specs().size());
    }

    @Test
    public void testInitOptionsWithHelp_optionalArgument() {
        parser.accepts("argName", "Funky description");

        OptionSet options = initOptionsWithHelp(parser, new String[]{"--argName"});
        assertNotNull(options);
        assertEquals(1, options.specs().size());
        assertTrue(options.has("argName"));
    }

    @Test(expected = CommandLineExitException.class)
    public void testInitOptionsWithHelp_missingArgument() {
        parser.accepts("argName", "Funky description").withRequiredArg().ofType(String.class).defaultsTo("nope");

        initOptionsWithHelp(parser, new String[]{"--argName"});
    }

    @Test(expected = ExitStatusZeroException.class)
    public void testPrintHelpAndExit() {
        printHelpAndExit(parser);
    }

    @Test(expected = CommandLineExitException.class)
    public void testPrintHelpAndExit_invalidOutputStream() {
        printHelpAndExit(parser, null);
    }
}
