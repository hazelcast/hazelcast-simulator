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
package com.hazelcast.simulator.common;

import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.CommandLineExitException;

import java.io.File;

import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static java.lang.String.format;

/**
 * Utility class to deal with the {@value #NAME} file.
 *
 * An agent entry can either be  a single address or two addresses separated with a comma.
 *
 * If you specify both addresses the first one is the public address. It is used by the Simulator Coordinator for SSH access.
 * The second address is the private address, which is used by Hazelcast instances to form the cluster.
 *
 * The address separation is needed to deal with cloud environments like EC2.
 */
public final class AgentsFile {

    public static final String NAME = "agents.txt";

    private AgentsFile() {
    }

    public static ComponentRegistry load(File agentFile) {
        ComponentRegistry componentRegistry = new ComponentRegistry();

        String content = fileAsText(agentFile);
        String[] addresses = content.split(NEW_LINE);
        int lineNumber = 1;
        for (String line : addresses) {
            int indexOfComment = line.indexOf('#');
            if (indexOfComment != -1) {
                line = line.substring(0, indexOfComment);
            }

            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }

            String[] chunks = line.split(",");
            switch (chunks.length) {
                case 1:
                    componentRegistry.addAgent(chunks[0], chunks[0]);
                    break;
                case 2:
                    componentRegistry.addAgent(chunks[0], chunks[1]);
                    break;
                default:
                    throw new CommandLineExitException(format("Line %s of file %s is invalid!"
                            + " It should contain one or two IP addresses separated by a comma,"
                            + " but it contains %s", lineNumber, agentFile, chunks.length));
            }
        }

        return componentRegistry;
    }

    public static void save(File agentsFile, ComponentRegistry registry) {
        StringBuilder sb = new StringBuilder();
        for (AgentData agentData : registry.getAgents()) {
            String publicAddress = agentData.getPublicAddress();
            String privateAddress = agentData.getPrivateAddress();

            if (publicAddress.equals(privateAddress)) {
                sb.append(publicAddress).append(NEW_LINE);
            } else {
                sb.append(publicAddress).append(',').append(privateAddress).append(NEW_LINE);
            }
        }
        writeText(sb.toString(), agentsFile);
    }
}
