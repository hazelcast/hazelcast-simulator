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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BashCommandTest {

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
    public void testExecute() {
        new BashCommand("pwd").execute();
    }

    @Test(expected = ExitStatusOneException.class)
    public void testExecute_exitStatus() {
        new BashCommand("pwd && false").execute();
    }

    @Test(expected = ScriptException.class)
    public void testExecute_withException() {
        new BashCommand("pwd && false").setThrowsException(true).execute();
    }
}