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

import com.hazelcast.simulator.coordinator.registry.AgentData;
import com.hazelcast.simulator.coordinator.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.TagUtils;

import java.io.File;
import java.util.Map;

import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static com.hazelcast.simulator.utils.TagUtils.tagsToString;
import static java.lang.String.format;

/**
 * Utility class to deal with the {@value #NAME} file.
 * <p>
 * An agent entry can either be  a single address or two addresses separated with a comma.
 * <p>
 * If you specify both addresses the first one is the public address. It is used by the Simulator Coordinator for SSH access.
 * The second address is the private address, which is used by Hazelcast instances to form the cluster.
 * <p>
 * The address separation is needed to deal with cloud environments like EC2.
 */
public final class AgentsFile {

    public static final String NAME = "agents.txt";

    private AgentsFile() {
    }

    public static ComponentRegistry load(File agentFile) {
        ComponentRegistry componentRegistry = new ComponentRegistry();

        String content = fileAsText(agentFile);
        String[] lines = content.split(NEW_LINE);
        int lineNumber = 0;
        for (String line : lines) {
            lineNumber++;

            line = cleanLine(line);

            if (line.isEmpty()) {
                continue;
            }

            int tagsIndex = line.indexOf('|');
            String addressesString = tagsIndex == -1 ? line : line.substring(0, tagsIndex);
            String tagsString = tagsIndex == -1 ? "" : line.substring(tagsIndex + 1);

            String publicIpAddress;
            String privateIpAddress;
            String[] addresses = addressesString.split(",");
            switch (addresses.length) {
                case 1:
                    publicIpAddress = addresses[0];
                    privateIpAddress = addresses[0];
                    break;
                case 2:
                    publicIpAddress = addresses[0];
                    privateIpAddress = addresses[1];
                    break;
                default:
                    throw new CommandLineExitException(format("Line %s of file %s is invalid!"
                            + " It should contain one or two IP addresses separated by a comma,"
                            + " but it contains %s", lineNumber, agentFile, addresses.length));
            }
            Map<String, String> tags = TagUtils.parseTags(tagsString);
            componentRegistry.addAgent(publicIpAddress, privateIpAddress, tags);
        }

        return componentRegistry;
    }

    private static String cleanLine(String line) {
        int indexOfComment = line.indexOf('#');
        if (indexOfComment != -1) {
            line = line.substring(0, indexOfComment);
        }

        line = line.trim();
        return line;
    }

    public static void save(File agentsFile, ComponentRegistry registry) {
        StringBuilder sb = new StringBuilder();
        for (AgentData agent : registry.getAgents()) {
            String publicAddress = agent.getPublicAddress();
            String privateAddress = agent.getPrivateAddress();

            if (publicAddress.equals(privateAddress)) {
                sb.append(publicAddress);
            } else {
                sb.append(publicAddress).append(',').append(privateAddress);
            }

            Map<String, String> tags = agent.getTags();
            if (!tags.isEmpty()) {
                sb.append('|').append(tagsToString(tags));
            }
            sb.append(NEW_LINE);
        }
        writeText(sb.toString(), agentsFile);
    }
}
