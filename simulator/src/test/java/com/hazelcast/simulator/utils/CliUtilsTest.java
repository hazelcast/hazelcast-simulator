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
