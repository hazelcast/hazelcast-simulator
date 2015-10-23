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

import static com.hazelcast.simulator.utils.NativeUtils.getPID;
import static com.hazelcast.simulator.utils.NativeUtils.kill;

@MessageSpec(value = "kill", description = "Instructs the receiving process to send a SIGKILL signal to itself.")
public class KillWorkerMessage extends RunnableMessage {

    private static final Logger LOGGER = Logger.getLogger(KillWorkerMessage.class);

    public KillWorkerMessage(MessageAddress messageAddress) {
        super(messageAddress);
    }

    @Override
    public void run() {
        int pid = getPID();
        LOGGER.info("I'm about to send kill -9 signal to myself. My PID is: " + pid);
        if (pid > 0) {
            kill(pid);
        }
    }

    @Override
    public boolean removeFromAgentList() {
        return true;
    }

    @Override
    public boolean disableMemberFailureDetection() {
        return true;
    }
}
