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
package com.hazelcast.simulator.utils;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.simulator.common.WorkerType;
import com.hazelcast.simulator.coordinator.ConfigFileTemplate;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.worker.MemberWorker;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.simulator.utils.CommonUtils.sleepMillisThrowException;
import static java.lang.String.format;

public final class HazelcastUtils {

    private static final int TIMEOUT_SECONDS = 60;
    private static final long PARTITION_WARMUP_TIMEOUT_NANOS = TimeUnit.MINUTES.toNanos(5);
    private static final int PARTITION_WARMUP_SLEEP_INTERVAL_MILLIS = 500;

    private static final Logger LOGGER = Logger.getLogger(MemberWorker.class);

    private HazelcastUtils() {
    }

    public static HazelcastInstance createServerHazelcastInstance(String hzConfigFile) throws Exception {
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(hzConfigFile);
        Config config = configBuilder.build();

        return Hazelcast.newHazelcastInstance(config);
    }

    public static HazelcastInstance createClientHazelcastInstance(String hzConfigFile) throws Exception {
        XmlClientConfigBuilder configBuilder = new XmlClientConfigBuilder(hzConfigFile);
        ClientConfig clientConfig = configBuilder.build();

        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    public static void warmupPartitions(HazelcastInstance hazelcastInstance) {
        LOGGER.info("Waiting for partition warmup");

        PartitionService partitionService = hazelcastInstance.getPartitionService();
        long started = System.nanoTime();
        for (Partition partition : partitionService.getPartitions()) {
            if (System.nanoTime() - started > PARTITION_WARMUP_TIMEOUT_NANOS) {
                throw new IllegalStateException("Partition warmup timeout. Partitions didn't get an owner in time");
            }

            while (partition.getOwner() == null) {
                LOGGER.debug("Partition owner is not yet set for partitionId: " + partition.getPartitionId());
                sleepMillisThrowException(PARTITION_WARMUP_SLEEP_INTERVAL_MILLIS);
            }
        }

        LOGGER.info("Partitions are warmed up successfully");
    }

    public static boolean isMaster(final HazelcastInstance hazelcastInstance, ScheduledExecutorService executor,
                                   int delaySeconds) {
        if (hazelcastInstance == null || !isOldestMember(hazelcastInstance)) {
            return false;
        }
        try {
            Callable<Boolean> callable = new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    return isOldestMember(hazelcastInstance);
                }
            };
            ScheduledFuture<Boolean> future = executor.schedule(callable, delaySeconds, TimeUnit.SECONDS);
            return future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        } catch (ExecutionException e) {
            throw new IllegalStateException(e);
        } catch (TimeoutException e) {
            throw new IllegalStateException(e);
        }
    }

    public static boolean isOldestMember(HazelcastInstance hazelcastInstance) {
        Iterator<Member> memberIterator = hazelcastInstance.getCluster().getMembers().iterator();
        return memberIterator.hasNext() && memberIterator.next().equals(hazelcastInstance.getLocalEndpoint());
    }

    public static String getHazelcastAddress(WorkerType workerType, String publicAddress, HazelcastInstance hazelcastInstance) {
        if (hazelcastInstance != null) {
            InetSocketAddress socketAddress = getInetSocketAddress(hazelcastInstance);
            if (socketAddress != null) {
                return socketAddress.getAddress().getHostAddress() + ':' + socketAddress.getPort();
            }
        }
        return (workerType == WorkerType.MEMBER ? "server:" : "client:") + publicAddress;
    }

    private static InetSocketAddress getInetSocketAddress(HazelcastInstance hazelcastInstance) {
        try {
            return (InetSocketAddress) hazelcastInstance.getLocalEndpoint().getSocketAddress();
        } catch (NoSuchMethodError ignored) {
            try {
                return hazelcastInstance.getCluster().getLocalMember().getInetSocketAddress();
            } catch (Exception e) {
                return null;
            }
        }
    }

    public static String initMemberHzConfig(String memberHzConfig,
                                            ComponentRegistry componentRegistry,
                                            String licenseKey,
                                            Map<String, String> env,
                                            boolean liteMember) {

        ConfigFileTemplate template = new ConfigFileTemplate(memberHzConfig);
        template.addEnvironment("licenseKey", licenseKey);
        template.addEnvironment(env);
        template.withComponentRegistry(componentRegistry);

        template.addReplacement("<!--MEMBERS-->",
                createAddressConfig("member", componentRegistry, env.get("HAZELCAST_PORT")));

        if (licenseKey != null) {
            template.addReplacement("<!--LICENSE-KEY-->", format("<license-key>%s</license-key>", licenseKey));
        }

        String manCenterURL = env.get("MANAGEMENT_CENTER_URL");
        if (!"none".equals(manCenterURL) && (manCenterURL.startsWith("http://") || manCenterURL.startsWith("https://"))) {
            String updateInterval = env.get("MANAGEMENT_CENTER_UPDATE_INTERVAL");
            String updateIntervalAttr = (updateInterval.isEmpty()) ? "" : " update-interval=\"" + updateInterval + '"';
            template.addReplacement("<!--MANAGEMENT_CENTER_CONFIG-->",
                    format("<management-center enabled=\"true\"%s>%n        %s%n" + "    </management-center>%n",
                            updateIntervalAttr, manCenterURL));
        }

        if (liteMember) {
            template.addReplacement("<!--LITE_MEMBER_CONFIG-->", "<lite-member enabled=\"true\"/>");
        }

        return template.render();
    }

    public static String initClientHzConfig(String clientHzConfig,
                                            ComponentRegistry componentRegistry,
                                            Map<String, String> env,
                                            String licenseKey) {
        ConfigFileTemplate template = new ConfigFileTemplate(clientHzConfig);
        template.withComponentRegistry(componentRegistry);
        template.addEnvironment("licenseKey", licenseKey);
        template.addEnvironment(env);

        template.addReplacement("<!--MEMBERS-->",
                createAddressConfig("address", componentRegistry, env.get("HAZELCAST_PORT")));
        if (licenseKey != null) {
            template.addReplacement("<!--LICENSE-KEY-->", format("<license-key>%s</license-key>", licenseKey));
        }

        return template.render();
    }

    static String createAddressConfig(String tagName, ComponentRegistry componentRegistry, String port) {
        StringBuilder members = new StringBuilder();
        for (AgentData agentData : componentRegistry.getAgents()) {
            String hostAddress = agentData.getPrivateAddress();
            members.append(format("<%s>%s:%s</%s>%n", tagName, hostAddress, port, tagName));
        }
        return members.toString();
    }
}
