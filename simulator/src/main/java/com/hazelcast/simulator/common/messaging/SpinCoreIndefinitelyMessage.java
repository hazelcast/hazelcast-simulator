/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.common.messaging;

import org.apache.log4j.Logger;

import java.util.Random;

@MessageSpec(value = "spinCore", description = "Triggers a number of threads who are spinning indefinitely.")
public class SpinCoreIndefinitelyMessage extends RunnableMessage {

    private static final Logger LOGGER = Logger.getLogger(SoftKillMessage.class);

    private final int noOfThreads;

    public SpinCoreIndefinitelyMessage(MessageAddress messageAddress) {
        this(messageAddress, 1);
    }

    public SpinCoreIndefinitelyMessage(MessageAddress messageAddress, int noOfThread) {
        super(messageAddress);
        this.noOfThreads = noOfThread;
    }

    @Override
    public void run() {
        for (int i = 0; i < noOfThreads; i++) {
            new BusySpinner().start();
        }
    }

    private static class BusySpinner extends Thread {

        private final Random random = new Random();

        @Override
        @SuppressWarnings("checkstyle:magicnumber")
        public void run() {
            while (!interrupted()) {
                if (random.nextInt(100) == 101) {
                    LOGGER.fatal("Can't happen!");
                }
            }
        }
    }
}
