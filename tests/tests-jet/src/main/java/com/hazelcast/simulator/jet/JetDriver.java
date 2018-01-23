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
package com.hazelcast.simulator.jet;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.simulator.agent.workerprocess.WorkerParameters;
import com.hazelcast.simulator.coordinator.ConfigFileTemplate;
import com.hazelcast.simulator.coordinator.registry.AgentData;
import com.hazelcast.simulator.utils.BashCommand;
import com.hazelcast.simulator.vendors.VendorDriver;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.CommonUtils.sleepMillisThrowException;
import static com.hazelcast.simulator.utils.FileUtils.getConfigurationFile;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static java.lang.String.format;


/**
 * todo
 * - gc log diagrams
 * - dstats diagrams
 * - version spec support
 * - configure the machines correctly
 *
 * done
 * - basics up and running
 * - fix the too many open files
 * - flamegraphs
 */
public class JetDriver extends VendorDriver<JetInstance> {
    private static final long PARTITION_WARMUP_TIMEOUT_NANOS = TimeUnit.MINUTES.toNanos(5);
    private static final int PARTITION_WARMUP_SLEEP_INTERVAL_MILLIS = 500;
    private static final Logger LOGGER = Logger.getLogger(JetDriver.class);
    private JetInstance jet;

    @Override
    public WorkerParameters loadWorkerParameters(String workerType, int agentIndex) {
        Map<String, String> s = new HashMap<String, String>(properties);
        s.remove("CONFIG");

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
        return get(argsProperty, "") + " -Dhazelcast.logging.type=log4j";
    }

    @Override
    public JetInstance getVendorInstance() {
        return jet;
    }

    public String initMemberHzConfig(boolean liteMember) {
        String config = loadMemberConfig();
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

        String manCenterURL = get("MANAGEMENT_CENTER_URL");
        if (!"none".equals(manCenterURL) && (manCenterURL.startsWith("http://") || manCenterURL.startsWith("https://"))) {
            String updateInterval = get("MANAGEMENT_CENTER_UPDATE_INTERVAL");
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

    private String loadMemberConfig() {
        String config = get("CONFIG");
        if (config != null) {
            return config;
        }

        return loadConfiguration("Hazelcast member configuration", "hazelcast.xml");
    }

    public String initClientHzConfig() {
        String config = loadClientConfig();

        ConfigFileTemplate template = new ConfigFileTemplate(config);
        //template.withAgents(componentRegistry);
        String licenseKey = get("LICENCE_KEY");
        template.addEnvironment("licenseKey", licenseKey);
        template.addEnvironment(properties);

        template.addReplacement("<!--MEMBERS-->",
                createAddressConfig("address", agents, get("HAZELCAST_PORT")));
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

        return loadConfiguration("Hazelcast client configuration", "client-hazelcast.xml");
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
        String cloud = get("CLOUD_PROVIDER");
        if ("embedded".equals(cloud)) {
            return;
        }

        String versionSpec = get("VERSION_SPEC");
        LOGGER.info("Installing versionSpec [" + versionSpec + "] on " + agents.size() + " agents...");

        String publicIps = "";
        if (!"local".equals(cloud)) {
            publicIps = AgentData.publicAddressesString(agents);
        }

        String vendor = get("VENDOR");
        String installFile = getConfigurationFile("install-" + vendor + ".sh").getPath();

        LOGGER.info("Installing '" + vendor + "' version '" + versionSpec + "' on Agents using " + installFile);

        new BashCommand(installFile)
                .addParams(get("SESSION_ID"), versionSpec, publicIps)
                .addEnvironment(properties)
                .execute();

        LOGGER.info("Successfully installed '" + vendor + "'");

        LOGGER.info("Install successful!");
    }

    @Override
    public void startVendorInstance() throws Exception {
        String workerType = get("WORKER_TYPE");

        LOGGER.info(format("%s HazelcastInstance starting", workerType));
        if ("javaclient".equals(workerType)) {
            File configFile = new File(getUserDir(), "client-hazelcast.xml");
            XmlClientConfigBuilder configBuilder = new XmlClientConfigBuilder(configFile);
            ClientConfig clientConfig = configBuilder.build();
            jet = Jet.newJetClient(clientConfig);
        } else {
            File configFile = new File(getUserDir(), "hazelcast.xml");
            XmlConfigBuilder configBuilder = new XmlConfigBuilder(configFile.getAbsolutePath());
            Config config = configBuilder.build();

            JetConfig jetConfig = new JetConfig();
            jetConfig.setHazelcastConfig(config);
            jet = Jet.newJetInstance(jetConfig);
        }
        LOGGER.info(format("%s HazelcastInstance started", workerType));
        warmupPartitions(jet.getHazelcastInstance());
        LOGGER.info("Warmed up partitions");
    }

    @Override
    public void close() throws IOException {
        LOGGER.info("Stopping HazelcastInstance...");

        if (jet != null) {
            jet.shutdown();
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
