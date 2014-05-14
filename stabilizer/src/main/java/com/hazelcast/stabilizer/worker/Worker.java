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
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.TestCase;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.agent.workerjvm.WorkerJvmManager;
import com.hazelcast.stabilizer.tests.DeleteTest;
import com.hazelcast.stabilizer.tests.TestDependencies;
import com.hazelcast.stabilizer.worker.testcommands.GenericTestCommand;
import com.hazelcast.stabilizer.worker.testcommands.GetOperationCountTestCommand;
import com.hazelcast.stabilizer.worker.testcommands.InitTestCommand;
import com.hazelcast.stabilizer.worker.testcommands.StartTestCommand;
import com.hazelcast.stabilizer.worker.testcommands.StopTestCommand;
import com.hazelcast.stabilizer.worker.testcommands.TestCommand;
import com.hazelcast.stabilizer.worker.testcommands.TestCommandRequest;
import com.hazelcast.stabilizer.worker.testcommands.TestCommandResponse;

import java.io.File;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.stabilizer.Utils.fileAsText;
import static com.hazelcast.stabilizer.Utils.getHostAddress;
import static com.hazelcast.stabilizer.Utils.writeObject;
import static com.hazelcast.stabilizer.tests.TestUtils.bindProperties;
import static java.lang.String.format;
import static java.util.Arrays.asList;

public class Worker {

    final static ILogger log = Logger.getLogger(Worker.class);

    private HazelcastInstance serverInstance;
    private HazelcastInstance clientInstance;

    private String hzFile;
    private String clientHzFile;

    private String workerMode;
    private String workerId;

    private volatile DeleteTest test;

    private final BlockingQueue<TestCommandRequest> requestQueue = new LinkedBlockingQueue<TestCommandRequest>();
    private final BlockingQueue<TestCommandResponse> responseQueue = new LinkedBlockingQueue<TestCommandResponse>();

    public void start() throws Exception {
        if ("server".equals(workerMode)) {
            this.serverInstance = createServerHazelcastInstance();
        } else if ("client".equals(workerMode)) {
            this.clientInstance = createClientHazelcastInstance();
        } else if ("mixed".equals(workerMode)) {
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
        if (serverInstance == null) {
            address = "client:" + getHostAddress();
        } else {
            InetSocketAddress socketAddress = serverInstance.getCluster().getLocalMember().getSocketAddress();
            address = socketAddress.getAddress().getHostAddress() + ":" + socketAddress.getPort();
        }
        File file = new File("worker.address");
        writeObject(address, file);
    }

    private HazelcastInstance createClientHazelcastInstance() throws Exception {
        log.info("Creating Client HazelcastInstance");

        XmlClientConfigBuilder configBuilder = new XmlClientConfigBuilder(clientHzFile);
        ClientConfig clientConfig = configBuilder.build();

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        log.info("Successfully created Client HazelcastInstance");
        return client;
    }

    private HazelcastInstance createServerHazelcastInstance() throws Exception {
        log.info("Creating Server HazelcastInstance");

        XmlConfigBuilder configBuilder = new XmlConfigBuilder(hzFile);
        Config config = configBuilder.build();

        HazelcastInstance server = Hazelcast.newHazelcastInstance(config);
        log.info("Successfully created Server HazelcastInstance");
        return server;
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
            logInputArguments();
            logInterestingSystemProperties();

            String workerId = System.getProperty("workerId");
            log.info("Worker id:" + workerId);

            String workerHzFile = args[0];
            log.info("Worker hz config file:" + workerHzFile);
            log.info(fileAsText(new File(workerHzFile)));

            String clientHzFile = args[1];
            log.info("Client hz config file:" + clientHzFile);
            log.info(fileAsText(new File(clientHzFile)));

            String workerMode = System.getProperty("workerMode");
            log.info("Worker mode:" + workerMode);

            Worker worker = new Worker();
            worker.workerId = workerId;
            worker.hzFile = workerHzFile;
            worker.clientHzFile = clientHzFile;
            worker.workerMode = workerMode;
            worker.start();

            log.info("Successfully started Hazelcast Stabilizer Worker:" + workerId);
        } catch (Throwable e) {
            ExceptionReporter.report(e);
            System.exit(1);
        }
    }

    private static void logInputArguments() {
        List<String> inputArguments = ManagementFactory.getRuntimeMXBean().getInputArguments();
        log.info("jvm input arguments = " + inputArguments);
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
                } catch (Throwable e) {
                    ExceptionReporter.report(e);
                }
            }
        }

        private void sendResponse(List<TestCommandResponse> responses) throws Exception {
            for (TestCommandResponse response : responses) {
                execute(WorkerJvmManager.COMMAND_PUSH_RESPONSE, workerId, response);
            }
        }

        //we create a new socket for every request because don't want to depend on the state of a socket
        //since we are going to do nasty stuff.
        private <E> E execute(String service, Object... args) throws Exception {
            Socket socket = new Socket(InetAddress.getByName(null), WorkerJvmManager.PORT);

            try {
                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                oos.writeObject(service);
                for (Object arg : args) {
                    oos.writeObject(arg);
                }
                oos.flush();

                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                Object response = in.readObject();

                if (response instanceof TerminateWorkerException) {
                    System.exit(0);
                }

                if (response instanceof Exception) {
                    Exception exception = (Exception) response;
                    Utils.fixRemoteStackTrace(exception, Thread.currentThread().getStackTrace());
                    throw exception;
                }
                return (E) response;
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
                    if (request == null) {
                        throw new NullPointerException("request can't be null");
                    }
                    doProcess(request.id, request.task);
                } catch (Throwable e) {
                    ExceptionReporter.report(e);
                }
            }
        }

        private void doProcess(long id, TestCommand command) {
            Object result = null;
            try {
                if (command instanceof InitTestCommand) {
                    process((InitTestCommand) command);
                } else if (command instanceof StartTestCommand) {
                    process((StartTestCommand) command);
                } else if (command instanceof StopTestCommand) {
                    process((StopTestCommand) command);
                } else if (command instanceof GenericTestCommand) {
                    result = process((GenericTestCommand) command);
                } else if (command instanceof GetOperationCountTestCommand) {
                    result = process((GetOperationCountTestCommand) command);
                } else {
                    throw new RuntimeException("Unhandled task:" + command.getClass());
                }
            } catch (Throwable e) {
                result = e;
            }

            TestCommandResponse response = new TestCommandResponse();
            response.commandId = id;
            response.result = result;
            responseQueue.add(response);
        }

        private Long process(GetOperationCountTestCommand command) {
            return test.getOperationCount();
        }

        private void process(StartTestCommand testCommand) throws Exception {
            try {
                log.info("Starting test");

                if (test == null) {
                    throw new IllegalStateException("No running test found");
                }

                boolean passive = false;
                if (testCommand.clientOnly && clientInstance == null) {
                    passive = true;
                }

                //test.run(passive);
            } catch (Exception e) {
                log.severe("Failed to start test", e);
                throw e;
            }
        }

        public Object process(GenericTestCommand genericTestTask) throws Throwable {
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
            } catch (InvocationTargetException e) {
                log.severe(format("Failed to execute test.%s()", methodName), e);
                throw e.getTargetException();
            } catch (Exception e) {
                log.severe(format("Failed to execute test.%s()", methodName), e);
                throw e;
            }
        }

        private void process(InitTestCommand initTestCommand) throws Exception {
            try {
                TestCase testCase = initTestCommand.testCase;
                log.info("Init Test:\n" + testCase);

                String clazzName = testCase.getClassname();

                TestDependencies dependencies = new TestDependencies();
                dependencies.clientInstance = clientInstance;
                dependencies.serverInstance = serverInstance;
                dependencies.testId = testCase.getId();

                test = (DeleteTest) InitTestCommand.class.getClassLoader().loadClass(clazzName).newInstance();
                test.init(dependencies);

                bindProperties(test, testCase);

                if (serverInstance != null) {
                    serverInstance.getUserContext().put(DeleteTest.TEST_INSTANCE, test);
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
               // test.stopTest(stopTask.timeoutMs);
                log.info("Finished calling test.stop()");
            } catch (Exception e) {
                log.severe("Failed to execute test.stop", e);
                throw e;
            }
        }
    }
}