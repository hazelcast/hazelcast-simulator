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
package com.hazelcast.stabilizer.worker;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.TestRecipe;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.agent.workerjvm.WorkerJvmManager;
import com.hazelcast.stabilizer.tests.Test;
import com.hazelcast.stabilizer.worker.testcommands.GenericTestCommand;
import com.hazelcast.stabilizer.worker.testcommands.InitTestCommand;
import com.hazelcast.stabilizer.worker.testcommands.StopTestCommand;
import com.hazelcast.stabilizer.worker.testcommands.TestCommand;
import com.hazelcast.stabilizer.worker.testcommands.TestCommandRequest;
import com.hazelcast.stabilizer.worker.testcommands.TestCommandResponse;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.stabilizer.Utils.asText;
import static com.hazelcast.stabilizer.Utils.getHostAddress;
import static com.hazelcast.stabilizer.Utils.writeObject;
import static com.hazelcast.stabilizer.tests.TestUtils.bindProperties;
import static java.lang.String.format;
import static java.util.Arrays.asList;

public class Worker {

    final static ILogger log = Logger.getLogger(Worker.class.getName());

    private HazelcastInstance serverInstance;
    private HazelcastInstance clientInstance;

    private String workerHzFile;
    private String workerMode;
    private String workerId;

    public volatile Test test;

    private BlockingQueue<TestCommandRequest> requestQueue = new LinkedBlockingQueue<TestCommandRequest>();
    private BlockingQueue<TestCommandResponse> responseQueue = new LinkedBlockingQueue<TestCommandResponse>();

    public void start() throws IOException {
        if ("server".equals(workerMode)) {
            this.serverInstance = createServerHazelcastInstance();
        } else if ("client".equals(workerMode)) {
            this.clientInstance = createClientHazelcastInstance();
        } else if ("combi".equals(workerMode)) {
            this.serverInstance = createServerHazelcastInstance();
            this.clientInstance = createClientHazelcastInstance();
        } else {
            throw new IllegalStateException("Unknown worker mode:" + workerMode);
        }

        new TestCommandRequestProcessingThread().start();
        new SocketThread().start();

        signalStartToAgent();
    }

    private void signalStartToAgent() {
        String address;
        if (serverInstance != null) {
            address = serverInstance.getCluster().getLocalMember().getSocketAddress().getHostString();
        } else {
            address = "client:" + getHostAddress();
        }
        File file = new File(workerId + ".address");
        writeObject(address, file);
    }

    private HazelcastInstance createClientHazelcastInstance() {
        log.info("Creating Client HazelcastInstance");

        Config config = loadServerConfig();

        //todo: instead of manually creating one, this should be send from the controller using a template
        ClientConfig clientConfig = new ClientConfig();
        GroupConfig clientGroupConfig = clientConfig.getGroupConfig();
        clientGroupConfig.setName(config.getGroupConfig().getName());
        clientGroupConfig.setPassword(config.getGroupConfig().getPassword());

        ClientNetworkConfig clientNetworkConfig = clientConfig.getNetworkConfig();
        for (String address : config.getNetworkConfig().getJoin().getTcpIpConfig().getMembers()) {
            clientNetworkConfig.addAddress(address);
        }

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        log.info("Successfully created Client HazelcastInstance");
        return client;
    }

    private HazelcastInstance createServerHazelcastInstance() {
        log.info("Creating Server HazelcastInstance");

        Config config = loadServerConfig();
        HazelcastInstance server = Hazelcast.newHazelcastInstance(config);
        log.info("Successfully created Server HazelcastInstance");
        return server;
    }

    private Config loadServerConfig() {
        XmlConfigBuilder configBuilder;
        try {
            configBuilder = new XmlConfigBuilder(workerHzFile);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        return configBuilder.build();
    }

    private static void logInterestingSystemProperties() {
        logSystemProperty("java.class.path");
        logSystemProperty("java.home");
        logSystemProperty("java.vendor");
        logSystemProperty("java.vendor.url");
        logSystemProperty("java.version");
        logSystemProperty("os.arch");
        logSystemProperty("os.name");
        logSystemProperty("os.version");
        logSystemProperty("user.dir");
        logSystemProperty("user.home");
        logSystemProperty("user.name");
        logSystemProperty("STABILIZER_HOME");
        logSystemProperty("hazelcast.logging.type");
        logSystemProperty("log4j.configuration");
        logSystemProperty("agent.mode");
    }

    private static void logSystemProperty(String name) {
        log.info(format("%s=%s", name, System.getProperty(name)));
    }

    public static void main(String[] args) {
        log.info("Starting Stabilizer Worker");
        try {
            logInterestingSystemProperties();

            String workerId = System.getProperty("workerId");
            log.info("Worker id:" + workerId);
            String workerHzFile = args[0];
            log.info("Worker hz config file:" + workerHzFile);
            log.info(asText(new File(workerHzFile)));

            String workerMode = System.getProperty("workerMode");
            log.info("Worker mode:" + workerMode);

            System.setProperty("workerId", workerId);

            Worker worker = new Worker();
            worker.workerId = workerId;
            worker.workerHzFile = workerHzFile;
            worker.workerMode = workerMode;
            worker.start();

            log.info("Successfully started Hazelcast Stabilizer Worker:" + workerId);
        } catch (Exception e) {
            log.severe(e);
            System.exit(1);
        }
    }

    private class SocketThread extends Thread {
        @Override
        public void run() {
            for (; ; ) {
                try {
                    List<TestCommandRequest> requests = execute(WorkerJvmManager.SERVICE_POLL_WORK, workerId);
                    for (TestCommandRequest request : requests) {
                        requestQueue.add(request);
                    }

                    TestCommandResponse response = responseQueue.poll(1, TimeUnit.SECONDS);
                    if (response == null) {
                        continue;
                    }

                    sendResponse(asList(response));

                    List<TestCommandResponse> responses = new LinkedList<TestCommandResponse>();
                    responseQueue.drainTo(responses);

                    sendResponse(responses);
                } catch (Exception e) {
                    log.severe(e);
                }
            }
        }

        private void sendResponse(List<TestCommandResponse> responses) throws Exception {
            for (TestCommandResponse response : responses) {
                execute(WorkerJvmManager.COMMAND_PUSH_RESPONSE, workerId, response);
            }
        }

        //we create a new socket for every request because don't want to depend on the state of a socket
        //because we are going to do nasty stuff.
        private <E> E execute(String service, Object... args) throws Exception {
            Socket socket = new Socket(InetAddress.getByName(null), 9001);

            try {
                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                oos.writeObject(service);
                for (Object arg : args) {
                    oos.writeObject(arg);
                }
                oos.flush();

                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                return (E) in.readObject();
            } finally {
                Utils.closeQuietly(socket);
            }
        }
    }

    private class TestCommandRequestProcessingThread extends Thread {

        @Override
        public void run() {
            for (; ; ) {
                try {
                    TestCommandRequest request = requestQueue.take();
                    doProcess(request.id, request.task);
                } catch (Exception e) {
                    log.severe(e);
                }
            }
        }

        private void doProcess(long id, TestCommand testCommand) {
            Object result = null;
            try {
                if (testCommand instanceof StopTestCommand) {
                    process((StopTestCommand) testCommand);
                } else if (testCommand instanceof InitTestCommand) {
                    process((InitTestCommand) testCommand);
                } else if (testCommand instanceof GenericTestCommand) {
                    result = process((GenericTestCommand) testCommand);
                } else {
                    throw new RuntimeException("Unhandled task:" + testCommand.getClass());
                }
            } catch (Throwable e) {
                result = e;
            }

            TestCommandResponse response = new TestCommandResponse();
            response.commandId = id;
            response.result = result;
            responseQueue.add(response);
        }

        public Object process(GenericTestCommand genericTestTask) throws Exception {
            String methodName = genericTestTask.methodName;
            try {
                log.info("Calling test." + methodName + "()");

                if (test == null) {
                    throw new IllegalStateException("No running test to execute test." + methodName + "()");
                }

                Method method = test.getClass().getMethod(methodName);
                Object o = method.invoke(test);
                log.info("Finished calling test." + methodName + "()");
                return o;
            } catch (Exception e) {
                log.severe(format("Failed to execute test.%s()", methodName), e);
                throw e;
            }
        }

        private void process(InitTestCommand initTestCommand) throws Exception {
            try {
                TestRecipe testRecipe = initTestCommand.testRecipe;
                log.info("Init Test:\n" + testRecipe);

                String clazzName = testRecipe.getClassname();

                test = (Test) InitTestCommand.class.getClassLoader().loadClass(clazzName).newInstance();
                test.setHazelcastInstances(serverInstance, clientInstance);
                test.setTestId(testRecipe.getTestId());

                bindProperties(test, testRecipe);

                if (serverInstance != null) {
                    serverInstance.getUserContext().put(Test.TEST_INSTANCE, test);
                }
            } catch (Exception e) {
                log.severe("Failed to init Test", e);
                throw e;
            }
        }

        public void process(StopTestCommand stopTask) throws Exception {
            try {
                log.info("Calling test.stop");

                if (test == null) {
                    throw new IllegalStateException("No test to stop");
                }
                test.stop(stopTask.timeoutMs);
                log.info("Finished calling test.stop()");
            } catch (Exception e) {
                log.severe("Failed to execute test.stop", e);
                throw e;
            }
        }
    }
}