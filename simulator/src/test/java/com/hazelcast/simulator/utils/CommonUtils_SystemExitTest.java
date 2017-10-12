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
import com.hazelcast.simulator.utils.helper.ExitStatusOneException;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;

public class CommonUtils_SystemExitTest {

    private static SecurityManager oldSecurityManager;

    private final Logger logger = Logger.getLogger(CommonUtils_SystemExitTest.class.getSimpleName());

    @BeforeClass
    public static void beforeClass() {
        oldSecurityManager = System.getSecurityManager();
        System.setSecurityManager(new ExitExceptionSecurityManager());
    }

    @AfterClass
    public static void afterClass() {
        System.setSecurityManager(oldSecurityManager);
    }

    @Test(expected = ExitStatusOneException.class)
    public void testExitWithError() {
        exitWithError();
    }

    @Test(expected = ExitStatusOneException.class)
    public void testExitWithError_withCommandLineExitException() {
        exitWithError(logger, "expected failure", new CommandLineExitException("expected CommandLineExitException",
                new RuntimeException("cause")));
    }

    @Test(expected = ExitStatusOneException.class)
    public void testExitWithError_withCommandLineExitException_noCause() {
        exitWithError(logger, "expected failure", new CommandLineExitException("expected CommandLineExitException"));
    }

    @Test(expected = ExitStatusOneException.class)
    public void testExitWithError_withThrowable() {
        exitWithError(logger, "expected failure", new Throwable("expected throwable"));
    }
}
