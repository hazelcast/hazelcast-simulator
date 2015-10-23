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

import static com.hazelcast.simulator.utils.NativeUtils.execute;

@MessageSpec(value = "blockHzTraffic", description = "Configures iptables to block all incoming traffic to TCP port range "
        + BlockTrafficMessage.PORTS + ". Requires sudo to be configured not ask for a password.")
public class BlockTrafficMessage extends RunnableMessage {

    static final String PORTS = "5700:5800";

    public BlockTrafficMessage(MessageAddress messageAddress) {
        super(messageAddress);
    }

    @Override
    public void run() {
        String command = String.format("sudo /sbin/iptables -p tcp --dport %s -A INPUT -i eth0 -j REJECT", PORTS);
        execute(command);
    }
}
