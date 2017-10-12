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
package com.hazelcast.simulator.harakiri;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.TestEnvironmentUtils.resetSecurityManager;
import static com.hazelcast.simulator.TestEnvironmentUtils.setExitExceptionSecurityManager;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_EC2;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_STATIC;
import static com.hazelcast.simulator.utils.CommonUtils.joinThread;
import static org.junit.Assert.assertEquals;

public class HarakiriMonitorTest {

    private static final int DEFAULT_TEST_TIMEOUT = 5000;
    private static final String CLOUD_PROVIDER = PROVIDER_STATIC;
    private static final String CLOUD_IDENTITY = "someIdentity";
    private static final String CLOUD_CREDENTIALS = "someCredentials";
    private static final int WAIT_SECONDS = 1;

    private HarakiriMonitor harakiriMonitor;

    @BeforeClass
    public static void before() {
        setExitExceptionSecurityManager();
    }

    @AfterClass
    public static void afterClass() {
        resetSecurityManager();
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testHarakiriMonitor_noEC2() {
        harakiriMonitor = new HarakiriMonitor(CLOUD_PROVIDER, CLOUD_IDENTITY, CLOUD_CREDENTIALS, WAIT_SECONDS);
        harakiriMonitor.start();
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testHarakiriMonitor_isEC2() {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        Thread thread = new Thread() {
            @Override
            public void run() {
                harakiriMonitor = new HarakiriMonitor(PROVIDER_EC2, "exit 0", WAIT_SECONDS);
                harakiriMonitor.start();
                countDownLatch.countDown();
            }
        };
        thread.start();
        joinThread(thread, TimeUnit.SECONDS.toMillis(WAIT_SECONDS * 2));

        assertEquals(0, countDownLatch.getCount());
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testHarakiriMonitor_isEC2_withFailure() {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        Thread thread = new Thread() {
            @Override
            public void run() {
                harakiriMonitor = new HarakiriMonitor(PROVIDER_EC2, "exit 1", WAIT_SECONDS);
                harakiriMonitor.start();
                countDownLatch.countDown();
            }
        };
        thread.start();
        joinThread(thread, TimeUnit.SECONDS.toMillis(WAIT_SECONDS * 2));

        assertEquals(1, countDownLatch.getCount());
    }
}
