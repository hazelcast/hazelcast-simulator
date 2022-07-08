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
package com.hazelcast.simulator.hazelcast3;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.simulator.agent.workerprocess.WorkerParameters;
import com.hazelcast.simulator.coordinator.ConfigFileTemplate;
import com.hazelcast.simulator.coordinator.registry.AgentData;
import com.hazelcast.simulator.utils.BashCommand;
import com.hazelcast.simulator.drivers.Driver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


import static com.hazelcast.simulator.utils.CommonUtils.sleepMillisThrowException;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.FileUtils.locatePythonFile;
import static java.lang.String.format;

public class Hazelcast3Driver extends Driver<HazelcastInstance> {
    private static final long PARTITION_WARMUP_TIMEOUT_NANOS = TimeUnit.MINUTES.toNanos(5);
    private static final int PARTITION_WARMUP_SLEEP_INTERVAL_MILLIS = 500;
    private static final Logger LOGGER = LogManager.getLogger(Hazelcast3Driver.class);
    private HazelcastInstance hazelcastInstance;

    @Override
    public WorkerParameters loadWorkerParameters(String workerType, int agentIndex) {
        Map<String, String> s = new HashMap<>(properties);
        s.remove("CONFIG");

        if ("hazelcast-enterprise3".equals(get("DRIVER"))) {
            String licenceKey = get("LICENCE_KEY");
            if (licenceKey == null) {
                throw new IllegalStateException("licenceKey needs to be set with 'hazelcast-enterprise3' as driver");
            }
        }

        WorkerParameters params = new WorkerParameters()
                .setAll(s)
                .set("WORKER_TYPE", workerType)
                .set("file:log4j.xml", loadLog4jConfig());

        if ("member".equals(workerType)) {
            loadMemberWorkerParameters(params);
        } else if ("javaclient".equals(workerType)) {
            loadJavaClientWorkerParameters(params);
        } else if ("litemember".equals(workerType)) {
            loadLiteMemberWorkerParameters(params);
        } else {
            throw new IllegalArgumentException(String.format("Unsupported workerType [%s]", workerType));
        }

        return params;
    }

    private void loadMemberWorkerParameters(WorkerParameters params) {
        params.set("JVM_OPTIONS", loadJvmOptions("MEMBER_ARGS"))
                .set("file:hazelcast.xml", initMemberHzConfig(false))
                .set("file:worker.sh", loadWorkerScript("member"));
    }

    private void loadLiteMemberWorkerParameters(WorkerParameters params) {
        params.set("JVM_OPTIONS", loadJvmOptions("CLIENT_ARGS"))
                .set("file:hazelcast.xml", initMemberHzConfig(true))
                .set("file:worker.sh", loadWorkerScript("litemember"));
    }

    private void loadJavaClientWorkerParameters(WorkerParameters params) {
        params.set("JVM_OPTIONS", loadJvmOptions("CLIENT_ARGS"))
                .set("file:client-hazelcast.xml", initClientHzConfig())
                .set("file:worker.sh", loadWorkerScript("javaclient"));
    }

    private String loadJvmOptions(String argsProperty) {
        return get(argsProperty, "");
    }

    @Override
    public HazelcastInstance getDriverInstance() {
        return hazelcastInstance;
    }

    public String initMemberHzConfig(boolean liteMember) {
        String config = loadMemberConfig(liteMember);
        ConfigFileTemplate template = new ConfigFileTemplate(config);

        String licenseKey = get("LICENCE_KEY");
        template.addEnvironment("licenseKey", licenseKey);
        template.addEnvironment(properties);
        //template.withAgents(componentRegistry);

        template.addReplacement("<!--MEMBERS-->",
                createAddressConfig("member", agents, get("HAZELCAST_PORT")));

        if (licenseKey != null) {
            template.addReplacement("<!--LICENSE-KEY-->", format("<license-key>%s</license-key>", licenseKey));
        }

        if (liteMember) {
            template.addReplacement("<!--LITE_MEMBER_CONFIG-->", "<lite-member enabled=\"true\"/>");
        }

        return template.render();
    }

    private String loadMemberConfig(boolean liteMember) {
        String config = get("CONFIG");
        if (config != null) {
            return config;
        }

        if (liteMember && new File("litemember-hazelcast.xml").exists()) {
            return loadConfigFile("Hazelcast lite-member configuration", "litemember-hazelcast.xml");
        } else {
            return loadConfigFile("Hazelcast member configuration", "hazelcast.xml");
        }
    }

    public String initClientHzConfig() {
        String config = loadClientConfig();

        ConfigFileTemplate template = new ConfigFileTemplate(config);
        //template.withAgents(componentRegistry);
        String licenseKey = get("LICENCE_KEY");
        template.addEnvironment("licenseKey", licenseKey);
        template.addEnvironment(properties);

        template.addReplacement("<!--MEMBERS-->",
                createAddressConfig("address", agents, get("CLIENT_PORT", get("HAZELCAST_PORT"))));
        if (licenseKey != null) {
            template.addReplacement("<!--LICENSE-KEY-->", format("<license-key>%s</license-key>", licenseKey));
        }

        return template.render();
    }

    private String loadClientConfig() {
        String config = get("CONFIG");
        if (config != null) {
            return config;
        }

        return loadConfigFile("Hazelcast client configuration", "client-hazelcast.xml");
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
    public void install() {
        String versionSpec = get("VERSION_SPEC");
        LOGGER.info("Installing versionSpec [" + versionSpec + "] on " + agents.size() + " agents...");

        String installFile = locatePythonFile("upload_hazelcast_jars.py");
        String driver = get("DRIVER");

        LOGGER.info("Installing '" + driver + "' version '" + versionSpec + "' on Agents using " + installFile);

        new BashCommand(installFile)
                .addParams(AgentData.toYaml(agents), versionSpec, driver)
                .addEnvironment(properties)
                .execute();

        LOGGER.info("Successfully installed '" + driver + "'");

        LOGGER.info("Install successful!");
    }

    @Override
    public void startDriverInstance() throws Exception {
        String workerType = get("WORKER_TYPE");

        LOGGER.info(format("%s HazelcastInstance starting", workerType));
        if ("javaclient".equals(workerType)) {
            File configFile = new File(getUserDir(), "client-hazelcast.xml");
            XmlClientConfigBuilder configBuilder = new XmlClientConfigBuilder(configFile);
            ClientConfig clientConfig = configBuilder.build();
            hazelcastInstance = HazelcastClient.newHazelcastClient(clientConfig);
        } else {
            File configFile = new File(getUserDir(), "hazelcast.xml");
            XmlConfigBuilder configBuilder = new XmlConfigBuilder(configFile.getAbsolutePath());
            Config config = configBuilder.build();
            hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        }
        LOGGER.info(format("%s HazelcastInstance started", workerType));
        warmupPartitions(hazelcastInstance);
        LOGGER.info("Warmed up partitions");
    }

    @Override
    public void close() throws IOException {
        LOGGER.info("Stopping HazelcastInstance...");

        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
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
