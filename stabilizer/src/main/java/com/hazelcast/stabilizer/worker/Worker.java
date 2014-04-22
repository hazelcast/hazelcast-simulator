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

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.TestRecipe;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.agent.WorkerJvmManager;
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
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.stabilizer.Utils.asText;
import static com.hazelcast.stabilizer.Utils.writeObject;
import static com.hazelcast.stabilizer.tests.TestUtils.bindProperties;
import static java.lang.String.format;
import static java.util.Arrays.asList;

public class Worker {

    final static ILogger log = Logger.getLogger(Worker.class.getName());

    private String workerId;
    private HazelcastInstance hz;
    private String workerHzFile;

    private BlockingQueue<TestCommandRequest> requestQueue = new LinkedBlockingQueue<TestCommandRequest>();
    private BlockingQueue<TestCommandResponse> responseQueue = new LinkedBlockingQueue<TestCommandResponse>();

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    public void setWorkerHzFile(String workerHzFile) {
        this.workerHzFile = workerHzFile;
    }

    public void start() throws IOException {
        Socket socket = new Socket(InetAddress.getByName(null), 10000);
        log.info("Socket created: " + socket.getRemoteSocketAddress());

        log.info("Creating Worker HazelcastInstance");
        this.hz = createHazelcastInstance();
        log.info("Successfully created Worker HazelcastInstance");

        new TestCommandRequestProcessingThread().start();
        new SocketThread().start();

        signalStartToAgent();
    }

    private void signalStartToAgent() {
        InetSocketAddress address = hz.getCluster().getLocalMember().getInetSocketAddress();
        File file = new File(workerId + ".address");
        writeObject(address, file);
    }

    private HazelcastInstance createHazelcastInstance() {
        XmlConfigBuilder configBuilder;
        try {
            configBuilder = new XmlConfigBuilder(workerHzFile);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        Config config = configBuilder.build();
        return Hazelcast.newHazelcastInstance(config);
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
    }

    private static void logSystemProperty(String name) {
        log.info(format("%s=%s", name, System.getProperty(name)));
    }

    public static void main(String[] args) {
        log.info("Starting Stabilizer Worker");
        try {
            logInterestingSystemProperties();

            String workerId = args[0];
            log.info("Worker id:" + workerId);
            String workerHzFile = args[1];
            log.info("Worker hz config file:" + workerHzFile);
            log.info(asText(new File(workerHzFile)));

            System.setProperty("workerId", workerId);

            Worker worker = new Worker();
            worker.setWorkerId(workerId);
            worker.setWorkerHzFile(workerHzFile);
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
                    responseQueue.drainTo(responseQueue);

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

        private <E> E execute(String service, Object... args) throws Exception {
            Socket socket = new Socket(InetAddress.getByName(null), 10000);

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

                Test test = (Test) hz.getUserContext().get(Test.TEST_INSTANCE);
                if (test == null) {
                    throw new IllegalStateException("No test found for method " + methodName + "()");
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

                Test test = (Test) InitTestCommand.class.getClassLoader().loadClass(clazzName).newInstance();
                test.setHazelcastInstance(hz);
                test.setTestId(testRecipe.getTestId());

                bindProperties(test, testRecipe);

                hz.getUserContext().put(Test.TEST_INSTANCE, test);
            } catch (Exception e) {
                log.severe("Failed to init Test", e);
                throw e;
            }
        }

        public void process(StopTestCommand stopTask) throws Exception {
            try {
                log.info("Calling test.stop");

                Test test = (Test) hz.getUserContext().get(Test.TEST_INSTANCE);
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