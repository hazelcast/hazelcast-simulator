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

import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;

@MessageSpec(value = "softKill", description = "Instructs receiving party to call System.exit(-1).")
public class SoftKillMessage extends RunnableMessage {

    private static final Logger LOGGER = Logger.getLogger(SoftKillMessage.class);

    public SoftKillMessage(MessageAddress messageAddress) {
        super(messageAddress);
    }

    @Override
    public void run() {
        LOGGER.warn("Processing soft kill message. I'm about to die!");
        exitWithError();
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
