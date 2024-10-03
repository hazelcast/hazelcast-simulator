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

import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.shaded.org.json.JSONArray;
import com.hazelcast.shaded.org.json.JSONObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class HazelcastUtils {

    private static final int TIMEOUT_SECONDS = 60;
    private static final Logger LOGGER = LogManager.getLogger(HazelcastUtils.class);

    private HazelcastUtils() {
    }

    public static boolean isMaster(final HazelcastInstance hazelcastInstance, ScheduledExecutorService executor,
                                   int delaySeconds) {
        if (hazelcastInstance == null || !isOldestMember(hazelcastInstance)) {
            return false;
        }
        try {
            Callable<Boolean> callable = () -> isOldestMember(hazelcastInstance);
            ScheduledFuture<Boolean> future = executor.schedule(callable, delaySeconds, TimeUnit.SECONDS);
            return future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new IllegalStateException(e);
        }
    }

    public static boolean isOldestMember(HazelcastInstance hazelcastInstance) {
        Iterator<Member> memberIterator = hazelcastInstance.getCluster().getMembers().iterator();
        return memberIterator.hasNext() && memberIterator.next().equals(hazelcastInstance.getLocalEndpoint());
    }

    /**
     * Handles configuration settings for a member, based on properties bound to an agent's internal IP address.
     * <p>
     * This method is designed to be extendable for future configuration
     * properties that may be applied on exclusive agents.
     * </p>
     *
     * <p>
     * Currently, the method supports the following configuration:
     * </p>
     * <ul>
     *   <li><b>CP Member Priority:</b> If the "cp_priority" property is provided,
     *   the method will set the CP member priority for the agent based on the agent's private IP address.</li>
     * </ul>
     *
     * <p>
     * Future configurations may extend the use of additional properties in a similar manner.
     * </p>
     *
     */
    public static void handlePerAgentConfig(Map<String, String> properties, Config config) {
        String cpPriorities = properties.get("cp_priority");
        if (cpPriorities == null) {
            return;
        }

        String agentPrivateAddress = properties.get("PRIVATE_ADDRESS");
        JSONArray cpPriorityArray = new JSONArray(cpPriorities);

        cpPriorityArray.forEach(item -> {
            JSONObject jsonObject = (JSONObject) item;
            String address = jsonObject.getString("address");
            int priority = jsonObject.getInt("priority");
            if (address.equals(agentPrivateAddress)) {
                LOGGER.info("Setting CP member priority to " + priority + " for agent " + agentPrivateAddress);
                config.getCPSubsystemConfig().setCPMemberPriority(priority);
            }
        });
    }

    public static String getHazelcastAddress(String workerType, String publicAddress, HazelcastInstance hazelcastInstance) {
        if (hazelcastInstance != null) {
            InetSocketAddress socketAddress = getInetSocketAddress(hazelcastInstance);
            if (socketAddress != null) {
                return socketAddress.getAddress().getHostAddress() + ':' + socketAddress.getPort();
            }
        }
        return (workerType.equals("member") ? "server:" : "client:") + publicAddress;
    }

    private static InetSocketAddress getInetSocketAddress(HazelcastInstance hazelcastInstance) {
        try {
            return (InetSocketAddress) hazelcastInstance.getLocalEndpoint().getSocketAddress();
        } catch (NoSuchMethodError ignored) {
            try {
                return hazelcastInstance.getCluster().getLocalMember().getSocketAddress();
            } catch (Exception e) {
                return null;
            }
        }
    }
}
