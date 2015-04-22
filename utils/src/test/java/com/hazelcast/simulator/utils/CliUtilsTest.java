package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.utils.helper.ExitException;
import com.hazelcast.simulator.utils.helper.ExitExceptionSecurityManager;
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
import static org.junit.Assert.fail;

public class CliUtilsTest {

    private final OptionParser parser = new OptionParser();

    private SecurityManager oldSecurityManager;

    @Before
    public void setUp() {
        oldSecurityManager = System.getSecurityManager();
        System.setSecurityManager(new ExitExceptionSecurityManager(true));
    }

    @After
    public void tearDown() {
        System.setSecurityManager(oldSecurityManager);
    }

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(CliUtils.class);
    }

    @Test
    public void testInitOptionsWithHelp_help() {
        try {
            initOptionsWithHelp(parser, new String[]{"--help"});
            fail("Expected ExitException for --help argument");
        } catch (ExitException e) {
            assertEquals(0, e.getStatus());
        }
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

    @Test
    public void testPrintHelpAndExit() {
        try {
            printHelpAndExit(parser);
            fail("Expected ExitException");
        } catch (ExitException e) {
            assertEquals(0, e.getStatus());
        }
    }

    @Test(expected = CommandLineExitException.class)
    public void testPrintHelpAndExit_invalidOutputStream() {
        printHelpAndExit(parser, null);
    }
}