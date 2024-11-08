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
package com.hazelcast.simulator.standaloneclient4plus;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.simulator.coordinator.registry.AgentData;
import com.hazelcast.simulator.drivers.Driver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.hazelcast.simulator.utils.CommonUtils.sleepMillisThrowException;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;

public class Standaloneclient4plusDriver extends Driver<HazelcastInstance> {
    private static final long PARTITION_WARMUP_TIMEOUT_NANOS = MINUTES.toNanos(5);
    private static final int PARTITION_WARMUP_SLEEP_INTERVAL_MILLIS = 500;
    private static final Logger LOGGER = LogManager.getLogger(Standaloneclient4plusDriver.class);
    private HazelcastInstance hazelcastInstance;

    @Override
    public HazelcastInstance getDriverInstance() {
        return hazelcastInstance;
    }

    public static String createAddressConfig(String tagName, List<AgentData> agents, String port) {
        StringBuilder members = new StringBuilder();
        for (AgentData agent : agents) {
            String hostAddress = agent.getPrivateAddress();
            members.append(format("<%s>%s:%s</%s>%n", tagName, hostAddress, port, tagName));
        }
        return members.toString();
    }

    @Override
    public void startDriverInstance() throws IOException {
        String workerType = get("WORKER_TYPE");

        LOGGER.info(BuildInfoProvider.getBuildInfo());

        LOGGER.info(format("%s HazelcastInstance starting", workerType));
        if ("javaclient".equals(workerType)) {
            File configFile = new File(getUserDir(), "client-hazelcast.xml");

            try {
                // this way of loading is preferred so that env-variables and sys properties are picked up
                System.setProperty("hazelcast.client.config", configFile.getAbsolutePath());
                ClientConfig config = ClientConfig.load();
                hazelcastInstance = HazelcastClient.newHazelcastClient(config);
            } catch (NoSuchMethodError e) {
                // Fall back in case ClientConfig.load doesn't exist (pre 4.2)
                LOGGER.warn("Defaulting to old XmlClientConfigBuilder style config loading");
                XmlClientConfigBuilder configBuilder = new XmlClientConfigBuilder(configFile);
                ClientConfig clientConfig = configBuilder.build();
                hazelcastInstance = HazelcastClient.newHazelcastClient(clientConfig);
            }
        } else {
            // member creation is not supported. Fail fast.
            LOGGER.fatal("The worker type can only be \"javaclient\" while using standaloneclient4Plus driver.");
            System.exit(1);
        }
        LOGGER.info(format("%s HazelcastInstance started", workerType));
        warmupPartitions(hazelcastInstance);
        LOGGER.info("Warmed up partitions");
    }

    @Override
    public void close() throws IOException {
        LOGGER.info("Stopping HazelcastInstance...");

        if (hazelcastInstance != null) {
            Thread t = new Thread() {
                @Override
                public void run() {
                    try {
                        hazelcastInstance.shutdown();
                    } catch (Exception e) {
                        LOGGER.error(e);
                    }
                }
            };
            t.start();
            try {
                t.join(MINUTES.toMillis(2));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
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
}
