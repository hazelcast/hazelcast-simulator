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

    private static final Logger log = Logger.getLogger(MemberWorker.class);

    private final BlockingQueue<CommandRequest> requestQueue = new LinkedBlockingQueue<CommandRequest>();
    private final BlockingQueue<CommandResponse> responseQueue = new LinkedBlockingQueue<CommandResponse>();

    private WorkerSocketProcessor workerSocketProcessor;
    private WorkerCommandRequestProcessor workerCommandRequestProcessor;

    private String hzFile;
    private String clientHzFile;

    private String workerMode;
    private String workerId;

    public static void main(String[] args) {
        log.info("Starting Simulator Worker");

        try {
            logInputArguments();
            logInterestingSystemProperties();

            String workerId = System.getProperty("workerId");
            log.info("Worker id: " + workerId);

            String workerHzFile = args[0];
            log.info("Worker hz config file: " + workerHzFile);
            log.info(fileAsText(new File(workerHzFile)));

            String clientHzFile = args[1];
            log.info("Client hz config file: " + clientHzFile);
            log.info(fileAsText(new File(clientHzFile)));

            String workerMode = System.getProperty("workerMode");
            log.info("Worker mode: " + workerMode);

            MemberWorker worker = new MemberWorker();
            worker.workerId = workerId;
            worker.hzFile = workerHzFile;
            worker.clientHzFile = clientHzFile;
            worker.workerMode = workerMode;
            worker.start();

            registerLog4jShutdownHandler(worker);

            log.info("Successfully started Hazelcast Simulator Worker: " + workerId);
        } catch (Throwable e) {
            ExceptionReporter.report(null, e);
            System.exit(1);
        }
    }

    private static void logInputArguments() {
        List<String> inputArguments = ManagementFactory.getRuntimeMXBean().getInputArguments();
        log.info("JVM input arguments: " + inputArguments);
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
        log.info(format("%s=%s", name, System.getProperty(name)));
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

    private MemberWorker() {
    }

    private void start() throws Exception {
        HazelcastInstance serverInstance = null;
        HazelcastInstance clientInstance = null;

        if ("server".equals(workerMode)) {
            log.info("------------------------------------------------------------------------");
            log.info("             member mode");
            log.info("------------------------------------------------------------------------");
            serverInstance = createServerHazelcastInstance();
            TestUtils.warmupPartitions(log, serverInstance);
        } else if ("client".equals(workerMode)) {
            log.info("------------------------------------------------------------------------");
            log.info("             client mode");
            log.info("------------------------------------------------------------------------");
            clientInstance = createClientHazelcastInstance();
            TestUtils.warmupPartitions(log, clientInstance);
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
        log.info("Stopping threads...");
        workerSocketProcessor.shutdown();
        workerCommandRequestProcessor.shutdown();
    }

    private HazelcastInstance createServerHazelcastInstance() throws Exception {
        log.info("Creating Server HazelcastInstance");

        XmlConfigBuilder configBuilder = new XmlConfigBuilder(hzFile);
        Config config = configBuilder.build();

        HazelcastInstance server = Hazelcast.newHazelcastInstance(config);
        log.info("Successfully created Server HazelcastInstance");
        return server;
    }

    private HazelcastInstance createClientHazelcastInstance() throws Exception {
        log.info("Creating Client HazelcastInstance");

        XmlClientConfigBuilder configBuilder = new XmlClientConfigBuilder(clientHzFile);
        ClientConfig clientConfig = configBuilder.build();

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        log.info("Successfully created Client HazelcastInstance");
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
}