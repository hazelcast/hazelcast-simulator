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
package com.hazelcast.simulator.common;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.utils.CommonUtils.await;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static org.junit.Assert.assertFalse;

public class ShutdownThreadTest {

    private CountDownLatch latch = new CountDownLatch(1);

    @Test
    public void testAwaitShutdown() {
        ShutdownThreadImpl thread = new ShutdownThreadImpl();
        thread.start();

        new UnblockThread().start();

        thread.awaitShutdown();
    }

    @Test
    public void testAwaitShutdownWithTimeout() {
        ShutdownThreadImpl thread = new ShutdownThreadImpl();
        thread.start();

        boolean success = thread.awaitShutdownWithTimeout();
        latch.countDown();
        thread.awaitShutdown();

        assertFalse(success);
    }

    private final class UnblockThread extends Thread {
        @Override
        public void run() {
            sleepMillis(100);
            latch.countDown();
        }
    }

    private final class ShutdownThreadImpl extends ShutdownThread {

        private ShutdownThreadImpl() {
            super("ShutdownThreadImpl", new AtomicBoolean(), false, 300);
        }

        @Override
        protected void doRun() {
            await(latch);
        }
    }
}
