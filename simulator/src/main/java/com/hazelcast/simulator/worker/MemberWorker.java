/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.simulator.test.utils.TestUtils;
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

import static com.hazelcast.simulator.utils.CommonUtils.getHostAddress;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.writeObject;
import static java.lang.String.format;

public class MemberWorker {

    private static final Logger LOGGER = Logger.getLogger(MemberWorker.class);

    private final BlockingQueue<CommandRequest> requestQueue = new LinkedBlockingQueue<CommandRequest>();
    private final BlockingQueue<CommandResponse> responseQueue = new LinkedBlockingQueue<CommandResponse>();

    private WorkerSocketProcessor workerSocketProcessor;
    private WorkerCommandRequestProcessor workerCommandRequestProcessor;

    private String hzFile;
    private String clientHzFile;

    private String workerMode;
    private String workerId;
    private boolean autoCreateHazelcastInstance = true;

    private MemberWorker() {
    }

    private void start() throws Exception {
        HazelcastInstance serverInstance = null;
        HazelcastInstance clientInstance = null;

        if (autoCreateHazelcastInstance && "server".equals(workerMode)) {
            LOGGER.info("------------------------------------------------------------------------");
            LOGGER.info("             member mode");
            LOGGER.info("------------------------------------------------------------------------");
            serverInstance = createServerHazelcastInstance();
            TestUtils.warmupPartitions(LOGGER, serverInstance);
        } else if (autoCreateHazelcastInstance && "client".equals(workerMode)) {
            LOGGER.info("------------------------------------------------------------------------");
            LOGGER.info("             client mode");
            LOGGER.info("------------------------------------------------------------------------");
            clientInstance = createClientHazelcastInstance();
            TestUtils.warmupPartitions(LOGGER, clientInstance);
        } else {
            throw new IllegalStateException("Unknown worker mode: " + workerMode);
        }

        workerSocketProcessor = new WorkerSocketProcessor(requestQueue, responseQueue, workerId);
        workerCommandRequestProcessor = new WorkerCommandRequestProcessor(requestQueue, responseQueue,
                serverInstance, clientInstance);

        // the last thing we do is to signal to the agent we have started
        signalStartToAgent(serverInstance);
    }

    private void stop() {
        LOGGER.info("Stopping threads...");
        workerSocketProcessor.shutdown();
        workerCommandRequestProcessor.shutdown();
    }

    private HazelcastInstance createServerHazelcastInstance() throws Exception {
        LOGGER.info("Creating Server HazelcastInstance");

        XmlConfigBuilder configBuilder = new XmlConfigBuilder(hzFile);
        Config config = configBuilder.build();

        HazelcastInstance server = Hazelcast.newHazelcastInstance(config);
        LOGGER.info("Successfully created Server HazelcastInstance");
        return server;
    }

    private HazelcastInstance createClientHazelcastInstance() throws Exception {
        LOGGER.info("Creating Client HazelcastInstance");

        XmlClientConfigBuilder configBuilder = new XmlClientConfigBuilder(clientHzFile);
        ClientConfig clientConfig = configBuilder.build();

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        LOGGER.info("Successfully created Client HazelcastInstance");
        return client;
    }

    private void signalStartToAgent(HazelcastInstance serverInstance) {
        String address;
        if (serverInstance != null) {
            InetSocketAddress socketAddress = serverInstance.getCluster().getLocalMember().getInetSocketAddress();
            address = socketAddress.getAddress().getHostAddress() + ":" + socketAddress.getPort();
        } else {
            address = "client:" + getHostAddress();
        }
        File file = new File("worker.address");
        writeObject(address, file);
    }

    public static void main(String[] args) {
        LOGGER.info("Starting Simulator Worker");

        try {
            logInputArguments();
            logInterestingSystemProperties();

            String workerId = System.getProperty("workerId");
            LOGGER.info("Worker id: " + workerId);

            String workerHzFile = args[0];
            LOGGER.info("Worker hz config file: " + workerHzFile);
            LOGGER.info(fileAsText(new File(workerHzFile)));

            String clientHzFile = args[1];
            LOGGER.info("Client hz config file: " + clientHzFile);
            LOGGER.info(fileAsText(new File(clientHzFile)));

            String workerMode = System.getProperty("workerMode");
            LOGGER.info("Worker mode: " + workerMode);

            String autoCreateHZInstances = System.getProperty("autoCreateHZInstances", "true");
            LOGGER.info("autoCreateHZInstances :" + autoCreateHZInstances);


            MemberWorker worker = new MemberWorker();
            worker.workerId = workerId;
            worker.hzFile = workerHzFile;
            worker.clientHzFile = clientHzFile;
            worker.workerMode = workerMode;
            worker.autoCreateHazelcastInstance = Boolean.parseBoolean(autoCreateHZInstances);
            worker.start();

            registerLog4jShutdownHandler(worker);

            LOGGER.info("Successfully started Hazelcast Simulator Worker: " + workerId);
        } catch (Throwable e) {
            ExceptionReporter.report(null, e);
            System.exit(1);
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

    private static void registerLog4jShutdownHandler(final MemberWorker worker) {
        // makes sure that log4j will always flush log-buffers
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                worker.stop();
                LogManager.shutdown();
            }
        });
    }
}
