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
package com.hazelcast.simulator.worker;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.worker.commands.CommandRequest;
import com.hazelcast.simulator.worker.commands.CommandResponse;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.fillString;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillisThrowException;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.writeObject;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;

public final class MemberWorker {

    private static final String DASHES = "---------------------------";

    private static final long PARTITION_WARMUP_TIMEOUT_NANOS = TimeUnit.MINUTES.toNanos(5);
    private static final int PARTITION_WARMUP_SLEEP_INTERVAL_MILLIS = 500;

    private static final Logger LOGGER = Logger.getLogger(MemberWorker.class);

    private final WorkerType type;
    private final String publicAddress;

    private final boolean autoCreateHzInstance;
    private final String hzConfigFile;

    private final WorkerSocketProcessor workerSocketProcessor;
    private final WorkerCommandRequestProcessor workerCommandRequestProcessor;
    //private final WorkerConnector workerConnector;

    private MemberWorker(String workerId, WorkerType type, String publicAddress, int agentIndex, int workerIndex, int workerPort,
                         boolean autoCreateHzInstance, String hConfigFile) throws Exception {
        this.type = type;
        this.publicAddress = publicAddress;

        this.autoCreateHzInstance = autoCreateHzInstance;
        this.hzConfigFile = hConfigFile;

        BlockingQueue<CommandRequest> requestQueue = new LinkedBlockingQueue<CommandRequest>();
        BlockingQueue<CommandResponse> responseQueue = new LinkedBlockingQueue<CommandResponse>();
        HazelcastInstance hazelcastInstance = getHazelcastInstance();

        this.workerSocketProcessor = new WorkerSocketProcessor(requestQueue, responseQueue, workerId);
        this.workerCommandRequestProcessor = new WorkerCommandRequestProcessor(requestQueue, responseQueue, type,
                hazelcastInstance);

        //workerConnector = WorkerConnector.createInstance(agentIndex, workerIndex, workerPort);
        //workerConnector.start();

        signalStartToAgent(hazelcastInstance);
    }

    private HazelcastInstance getHazelcastInstance() throws Exception {
        HazelcastInstance hazelcastInstance = null;
        if (autoCreateHzInstance) {
            logHeader("Creating " + type + " HazelcastInstance");
            switch (type) {
                case MEMBER:
                    hazelcastInstance = createServerHazelcastInstance();
                    break;

                case CLIENT:
                    hazelcastInstance = createClientHazelcastInstance();
                    break;

                default:
                    throw new IllegalStateException("Unknown WorkerType: " + type);
            }
            logHeader("Successfully created " + type + " HazelcastInstance");

            warmupPartitions(hazelcastInstance);
        }
        return hazelcastInstance;
    }

    private HazelcastInstance createServerHazelcastInstance() throws Exception {
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(hzConfigFile);
        Config config = configBuilder.build();

        return Hazelcast.newHazelcastInstance(config);
    }

    private HazelcastInstance createClientHazelcastInstance() throws Exception {
        XmlClientConfigBuilder configBuilder = new XmlClientConfigBuilder(hzConfigFile);
        ClientConfig clientConfig = configBuilder.build();

        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    private void warmupPartitions(HazelcastInstance hz) {
        LOGGER.info("Waiting for partition warmup");

        PartitionService partitionService = hz.getPartitionService();
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

    private void signalStartToAgent(HazelcastInstance serverInstance) {
        String address;
        if (type == WorkerType.MEMBER) {
            InetSocketAddress socketAddress = serverInstance.getCluster().getLocalMember().getInetSocketAddress();
            address = socketAddress.getAddress().getHostAddress() + ":" + socketAddress.getPort();
        } else {
            address = "client:" + publicAddress;
        }
        File file = new File("worker.address");
        writeObject(address, file);
    }

    private void stop() {
        LOGGER.info("Stopping threads...");
        //workerConnector.shutdown();
        workerSocketProcessor.shutdown();
        workerCommandRequestProcessor.shutdown();
    }

    public static void main(String[] args) {
        LOGGER.info("Starting Hazelcast Simulator Worker");

        try {
            String workerId = System.getProperty("workerId");
            WorkerType type = WorkerType.valueOf(System.getProperty("workerType"));

            String publicAddress = System.getProperty("publicAddress");
            int agentIndex = parseInt(System.getProperty("agentIndex"));
            int workerIndex = parseInt(System.getProperty("workerIndex"));
            int workerPort = parseInt(System.getProperty("workerPort"));
            String hzConfigFile = System.getProperty("hzConfigFile");

            boolean autoCreateHzInstance = parseBoolean(System.getProperty("autoCreateHzInstance", "true"));

            logHeader("Hazelcast Worker #" + workerIndex + " (" + type + ")");
            logInputArguments();
            logInterestingSystemProperties();

            LOGGER.info("Worker id: " + workerId);
            LOGGER.info("Worker type: " + type);

            LOGGER.info("Public address: " + publicAddress);
            LOGGER.info("Agent index: " + agentIndex);
            LOGGER.info("Worker index: " + workerIndex);
            LOGGER.info("Worker port: " + workerPort);

            LOGGER.info("autoCreateHzInstance: " + autoCreateHzInstance);

            LOGGER.info("Hazelcast config file: " + hzConfigFile);
            LOGGER.info(fileAsText(new File(hzConfigFile)));

            MemberWorker worker = new MemberWorker(workerId, type, publicAddress, agentIndex, workerIndex, workerPort,
                    autoCreateHzInstance, hzConfigFile);

            registerLog4jShutdownHandler(worker);

            logHeader("Successfully started Hazelcast Worker #" + workerIndex);
        } catch (Exception e) {
            ExceptionReporter.report(null, e);
            exitWithError(LOGGER, "Could not start Hazelcast Simulator Worker!", e);
        }
    }

    private static void logInputArguments() {
        List<String> inputArguments = ManagementFactory.getRuntimeMXBean().getInputArguments();
        LOGGER.info("JVM input arguments: " + inputArguments);
    }

    private static void logInterestingSystemProperties() {
        logSystemProperty("java.class.path");
        logSystemProperty("java.home");
        logSystemProperty("java.vendor");
        logSystemProperty("java.vendor.url");
        logSystemProperty("sun.java.command");
        logSystemProperty("java.version");
        logSystemProperty("os.arch");
        logSystemProperty("os.name");
        logSystemProperty("os.version");
        logSystemProperty("user.dir");
        logSystemProperty("user.home");
        logSystemProperty("user.name");
        logSystemProperty("SIMULATOR_HOME");
        logSystemProperty("hazelcast.logging.type");
        logSystemProperty("log4j.configuration");
    }

    private static void logSystemProperty(String name) {
        LOGGER.info(format("%s=%s", name, System.getProperty(name)));
    }

    private static void logHeader(String header) {
        StringBuilder builder = new StringBuilder();
        builder.append(DASHES).append(" ").append(header).append(" ").append(DASHES);

        String dashes = fillString(builder.length(), '-');
        LOGGER.info(dashes);
        LOGGER.info(builder.toString());
        LOGGER.info(dashes);
    }

    private static void registerLog4jShutdownHandler(final MemberWorker worker) {
        // makes sure that log4j will always flush the log buffers
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                worker.stop();
                LogManager.shutdown();
            }
        });
    }
}
