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
import com.hazelcast.simulator.protocol.connector.WorkerConnector;
import com.hazelcast.simulator.protocol.processors.WorkerOperationProcessor;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.worker.performance.WorkerPerformanceMonitor;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillisThrowException;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.writeObject;
import static com.hazelcast.simulator.utils.FormatUtils.fillString;
import static com.hazelcast.simulator.utils.NativeUtils.getPID;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;

public final class MemberWorker implements Worker {

    private static final String DASHES = "---------------------------";

    private static final long PARTITION_WARMUP_TIMEOUT_NANOS = TimeUnit.MINUTES.toNanos(5);
    private static final int PARTITION_WARMUP_SLEEP_INTERVAL_MILLIS = 500;

    private static final Logger LOGGER = Logger.getLogger(MemberWorker.class);
    private static final AtomicBoolean SHUTDOWN_STARTED = new AtomicBoolean();

    private final WorkerType type;
    private final String publicAddress;

    private final boolean autoCreateHzInstance;
    private final String hzConfigFile;

    private final HazelcastInstance hazelcastInstance;
    private final WorkerConnector workerConnector;

    private final WorkerPerformanceMonitor workerPerformanceMonitor;

    private ShutdownThread shutdownThread;

    MemberWorker(WorkerType type, String publicAddress, int agentIndex, int workerIndex, int workerPort,
                 boolean autoCreateHzInstance, int workerPerformanceMonitorIntervalSeconds, String hConfigFile) throws Exception {
        SHUTDOWN_STARTED.set(false);

        this.type = type;
        this.publicAddress = publicAddress;

        this.autoCreateHzInstance = autoCreateHzInstance;
        this.hzConfigFile = hConfigFile;

        this.hazelcastInstance = getHazelcastInstance();

        this.workerConnector = WorkerConnector.createInstance(agentIndex, workerIndex, workerPort, type, hazelcastInstance, this);
        this.workerConnector.start();

        this.workerPerformanceMonitor = initWorkerPerformanceMonitor(workerPerformanceMonitorIntervalSeconds);

        Runtime.getRuntime().addShutdownHook(new ShutdownThread(true));

        signalStartToAgent(hazelcastInstance);
    }

    private WorkerPerformanceMonitor initWorkerPerformanceMonitor(int workerPerformanceMonitorIntervalSeconds) {
        if (workerPerformanceMonitorIntervalSeconds < 1) {
            return null;
        }
        WorkerOperationProcessor processor = (WorkerOperationProcessor) workerConnector.getProcessor();
        return new WorkerPerformanceMonitor(workerConnector, processor.getTests(), workerPerformanceMonitorIntervalSeconds);
    }

    @Override
    public void shutdown() {
        shutdownThread = new ShutdownThread(false);
        shutdownThread.start();
    }

    // just for testing
    void awaitShutdown() throws Exception {
        if (shutdownThread != null) {
            shutdownThread.awaitShutdown();
        }
    }

    @Override
    public boolean startPerformanceMonitor() {
        if (workerPerformanceMonitor == null) {
            return false;
        }
        return workerPerformanceMonitor.start();
    }

    @Override
    public void shutdownPerformanceMonitor() {
        if (workerPerformanceMonitor != null) {
            workerPerformanceMonitor.shutdown();
        }
    }

    @Override
    public WorkerConnector getWorkerConnector() {
        return workerConnector;
    }

    private HazelcastInstance getHazelcastInstance() throws Exception {
        HazelcastInstance instance = null;
        if (autoCreateHzInstance) {
            logHeader("Creating " + type + " HazelcastInstance");
            switch (type) {
                case CLIENT:
                    instance = createClientHazelcastInstance();
                    break;
                default:
                    instance = createServerHazelcastInstance();
            }
            logHeader("Successfully created " + type + " HazelcastInstance");

            warmupPartitions(instance);
        }
        return instance;
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
            if (serverInstance != null) {
                InetSocketAddress socketAddress = serverInstance.getCluster().getLocalMember().getInetSocketAddress();
                address = socketAddress.getAddress().getHostAddress() + ':' + socketAddress.getPort();
            } else {
                address = "server:" + publicAddress;
            }
        } else {
            address = "client:" + publicAddress;
        }
        File file = new File("worker.address");
        writeObject(address, file);
    }

    public static void main(String[] args) {
        try {
            startWorker();
        } catch (Exception e) {
            ExceptionReporter.report(null, e);
            exitWithError(LOGGER, "Could not start Hazelcast Simulator Worker!", e);
        }
    }

    static MemberWorker startWorker() throws Exception {
        LOGGER.info("Starting Hazelcast Simulator Worker");

        String workerId = System.getProperty("workerId");
        WorkerType type = WorkerType.valueOf(System.getProperty("workerType"));

        String publicAddress = System.getProperty("publicAddress");
        int agentIndex = parseInt(System.getProperty("agentIndex"));
        int workerIndex = parseInt(System.getProperty("workerIndex"));
        int workerPort = parseInt(System.getProperty("workerPort"));
        String hzConfigFile = System.getProperty("hzConfigFile");

        boolean autoCreateHzInstance = parseBoolean(System.getProperty("autoCreateHzInstance", "true"));
        int workerPerformanceMonitorIntervalSeconds = parseInt(System.getProperty("workerPerformanceMonitorIntervalSeconds"));

        logHeader("Hazelcast Worker #" + workerIndex + " (" + type + ')');
        logInputArguments();
        logInterestingSystemProperties();
        LOGGER.info("process ID: " + getPID());

        LOGGER.info("Worker id: " + workerId);
        LOGGER.info("Worker type: " + type);

        LOGGER.info("Public address: " + publicAddress);
        LOGGER.info("Agent index: " + agentIndex);
        LOGGER.info("Worker index: " + workerIndex);
        LOGGER.info("Worker port: " + workerPort);

        LOGGER.info("autoCreateHzInstance: " + autoCreateHzInstance);
        LOGGER.info("workerPerformanceMonitorIntervalSeconds: " + workerPerformanceMonitorIntervalSeconds);

        LOGGER.info("Hazelcast config file: " + hzConfigFile);
        LOGGER.info(fileAsText(new File(hzConfigFile)));

        MemberWorker worker = new MemberWorker(type, publicAddress, agentIndex, workerIndex, workerPort, autoCreateHzInstance,
                workerPerformanceMonitorIntervalSeconds, hzConfigFile);

        logHeader("Successfully started Hazelcast Worker #" + workerIndex);

        return worker;
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
        builder.append(DASHES).append(' ').append(header).append(' ').append(DASHES);

        String dashes = fillString(builder.length(), '-');
        LOGGER.info(dashes);
        LOGGER.info(builder.toString());
        LOGGER.info(dashes);
    }

    private final class ShutdownThread extends Thread {

        private final CountDownLatch shutdownComplete = new CountDownLatch(1);

        private final boolean shutdownLog4j;

        public ShutdownThread(boolean shutdownLog4j) {
            super("WorkerShutdownThread");
            setDaemon(true);

            this.shutdownLog4j = shutdownLog4j;
        }

        public void awaitShutdown() throws Exception {
            shutdownComplete.await();
        }

        @Override
        public void run() {
            if (!SHUTDOWN_STARTED.compareAndSet(false, true)) {
                return;
            }

            LOGGER.info("Stopping HazelcastInstance...");
            if (hazelcastInstance != null) {
                hazelcastInstance.shutdown();
            }

            LOGGER.info("Stopping WorkerPerformanceMonitor");
            if (workerPerformanceMonitor != null) {
                workerPerformanceMonitor.shutdown();
            }

            LOGGER.info("Stopping WorkerConnector...");
            if (workerConnector != null) {
                workerConnector.shutdown();
            }

            if (shutdownLog4j) {
                // makes sure that log4j will always flush the log buffers
                LOGGER.info("Stopping log4j...");
                LogManager.shutdown();
            }

            shutdownComplete.countDown();
        }
    }
}
