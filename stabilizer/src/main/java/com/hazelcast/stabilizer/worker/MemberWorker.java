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
import com.hazelcast.stabilizer.common.messaging.Message;
import com.hazelcast.stabilizer.probes.probes.ProbesConfiguration;
import com.hazelcast.stabilizer.probes.probes.Result;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.utils.ExceptionReporter;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.worker.commands.Command;
import com.hazelcast.stabilizer.worker.commands.CommandRequest;
import com.hazelcast.stabilizer.worker.commands.CommandResponse;
import com.hazelcast.stabilizer.worker.commands.GenericCommand;
import com.hazelcast.stabilizer.worker.commands.GetBenchmarkResultsCommand;
import com.hazelcast.stabilizer.worker.commands.GetOperationCountCommand;
import com.hazelcast.stabilizer.worker.commands.InitCommand;
import com.hazelcast.stabilizer.worker.commands.IsPhaseCompletedCommand;
import com.hazelcast.stabilizer.worker.commands.MessageCommand;
import com.hazelcast.stabilizer.worker.commands.RunCommand;
import com.hazelcast.stabilizer.worker.commands.StopCommand;
import org.apache.log4j.LogManager;

import java.io.File;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.stabilizer.Utils.fileAsText;
import static com.hazelcast.stabilizer.Utils.getHostAddress;
import static com.hazelcast.stabilizer.Utils.writeObject;
import static com.hazelcast.stabilizer.tests.utils.PropertyBindingSupport.bindProperties;
import static com.hazelcast.stabilizer.tests.utils.PropertyBindingSupport.parseProbeConfiguration;
import static java.lang.String.format;
import static java.util.Arrays.asList;

public class MemberWorker {

	private static final String DASHES = "---------------------------";
	private static final ILogger log = Logger.getLogger(MemberWorker.class);

	private final ConcurrentMap<String, Command> commands = new ConcurrentHashMap<String, Command>();
	private final ConcurrentMap<String, TestContainer<TestContext>> tests
            = new ConcurrentHashMap<String, TestContainer<TestContext>>();

    private final WorkerMessageProcessor workerMessageProcessor = new WorkerMessageProcessor(tests);

    private final BlockingQueue<CommandRequest> requestQueue = new LinkedBlockingQueue<CommandRequest>();
    private final BlockingQueue<CommandResponse> responseQueue = new LinkedBlockingQueue<CommandResponse>();

	private HazelcastInstance serverInstance;
	private HazelcastInstance clientInstance;

	private String hzFile;
	private String clientHzFile;

	private String workerMode;
	private String workerId;

	public void start() throws Exception {
        if ("server".equals(workerMode)) {
            log.info("------------------------------------------------------------------------");
            log.info("             member mode");
            log.info("------------------------------------------------------------------------");
            this.serverInstance = createServerHazelcastInstance();
            TestUtils.warmupPartitions(log, serverInstance);
        } else if ("client".equals(workerMode)) {
            log.info("------------------------------------------------------------------------");
            log.info("             client mode");
            log.info("------------------------------------------------------------------------");
            this.clientInstance = createClientHazelcastInstance();
            TestUtils.warmupPartitions(log, clientInstance);
        } else {
            throw new IllegalStateException("Unknown worker mode:" + workerMode);
        }
        log.info("------------------------------------------------------------------------");

        workerMessageProcessor.setHazelcastServerInstance(serverInstance);
        workerMessageProcessor.setHazelcastClientInstance(clientInstance);

        new CommandRequestProcessingThread().start();
        new SocketThread().start();
        new PerformanceMonitorThread().start();

        // the last thing we do is to signal to the agent we have started.
        signalStartToAgent();
    }

    private void signalStartToAgent() {
        String address;
        if (serverInstance == null) {
            address = "client:" + getHostAddress();
        } else {
            InetSocketAddress socketAddress = serverInstance.getCluster().getLocalMember().getInetSocketAddress();
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

    protected static void logInterestingSystemProperties() {
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
        registerLog4jShutdownHandler();

        log.info("Starting Stabilizer Worker");

        registerLog4jShutdownHandler();

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

            MemberWorker worker = new MemberWorker();
            worker.workerId = workerId;
            worker.hzFile = workerHzFile;
            worker.clientHzFile = clientHzFile;
            worker.workerMode = workerMode;
            worker.start();

            log.info("Successfully started Hazelcast Stabilizer Worker:" + workerId);
        } catch (Throwable e) {
            ExceptionReporter.report(null, e);
            System.exit(1);
        }
    }

    private static void registerLog4jShutdownHandler() {
        // makes sure that log4j will always flush log-buffers
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                LogManager.shutdown();
            }
        });
    }

    protected static void logInputArguments() {
        List<String> inputArguments = ManagementFactory.getRuntimeMXBean().getInputArguments();
        log.info("jvm input arguments = " + inputArguments);
    }

    private class SocketThread extends Thread {

        @Override
        public void run() {
            for (; ; ) {
                try {
                    List<CommandRequest> requests = execute(WorkerJvmManager.SERVICE_POLL_WORK, workerId);
                    for (CommandRequest request : requests) {
                        requestQueue.add(request);
                    }

                    CommandResponse response = responseQueue.poll(1, TimeUnit.SECONDS);
                    if (response == null) {
                        continue;
                    }

                    sendResponse(asList(response));

                    List<CommandResponse> responses = new LinkedList<CommandResponse>();
                    responseQueue.drainTo(responses);

                    sendResponse(responses);
                } catch (Throwable e) {
                    ExceptionReporter.report(null, e);
                }
            }
        }

        private void sendResponse(List<CommandResponse> responses) throws Exception {
            for (CommandResponse response : responses) {
                execute(WorkerJvmManager.COMMAND_PUSH_RESPONSE, workerId, response);
            }
        }

        // we create a new socket for every request because don't want to depend on the state of a socket
        // since we are going to do nasty stuff.
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

    private class CommandRequestProcessingThread extends Thread {

        @Override
        public void run() {
            for (; ; ) {
                try {
                    CommandRequest request = requestQueue.take();
                    if (request == null) {
                        throw new NullPointerException("request can't be null");
                    }
                    doProcess(request.id, request.task);
                } catch (Throwable e) {
                    ExceptionReporter.report(null, e);
                }
            }
        }

        private void doProcess(long id, Command command) throws Throwable {
            Object result = null;
            try {
                if (command instanceof IsPhaseCompletedCommand) {
                    result = process((IsPhaseCompletedCommand) command);
                } else if (command instanceof InitCommand) {
                    process((InitCommand) command);
                } else if (command instanceof RunCommand) {
                    process((RunCommand) command);
                } else if (command instanceof StopCommand) {
                    process((StopCommand) command);
                } else if (command instanceof GenericCommand) {
                    process((GenericCommand) command);
                } else if (command instanceof GetOperationCountCommand) {
                    result = process((GetOperationCountCommand) command);
                } else if (command instanceof GetBenchmarkResultsCommand) {
                    result = process((GetBenchmarkResultsCommand) command);
                } else if (command instanceof MessageCommand) {
                    process((MessageCommand) command);
                } else {
                    throw new RuntimeException("Unhandled task:" + command.getClass());
                }
            } finally {
                if (command.awaitReply()) {
                    CommandResponse response = new CommandResponse();
                    response.commandId = id;
                    response.result = result;
                    responseQueue.add(response);
                }
            }
        }

        private Map<String, Result<?>> process(GetBenchmarkResultsCommand command) {
            String testId = command.getTestId();
            return tests.get(testId).getProbeResults();
        }

        private void process(MessageCommand command) {
            Message message = command.getMessage();
            workerMessageProcessor.submit(message);
        }

        private Long process(GetOperationCountCommand command) throws Throwable {
            long result = 0;

            for (TestContainer testContainer : tests.values()) {
                result += testContainer.getOperationCount();
            }

            return result;
        }

        private void process(final RunCommand command) throws Exception {
            try {
                final String testId = command.testId;
                final String testName = "".equals(testId) ? "test" : testId;

                final TestContainer<TestContext> test = tests.get(testId);
                if (test == null) {
                    log.warning("Failed to process command: " + command + " no test with testId" + testId + " is found");
                    return;
                }

                CommandThread commandThread = new CommandThread(command, testId) {
                    @Override
                    public void doRun() throws Throwable {
                        boolean passive = command.clientOnly && clientInstance == null;

                        if (passive) {
                            log.info(format("%s Skipping %s.run() (member is passive) %s", DASHES, testName, DASHES));
                        } else {
                            log.info(format("%s Starting %s.run() %s", DASHES, testId, DASHES));

                            try {
                                test.run();
                                log.info(format("%s Completed %s.run() %s", DASHES, testName, DASHES));
                            } catch (InvocationTargetException e) {
                                log.severe(format("%s Failed to execute %s.run() %s", DASHES, testName, DASHES), e.getCause());
                                throw e.getCause();
                            }
                        }
                    }
                };
                commandThread.start();
            } catch (Exception e) {
                log.severe("Failed to start test", e);
                throw e;
            }
        }

        public void process(final GenericCommand command) throws Throwable {
            final String methodName = command.methodName;
            final String testId = command.testId;
            final String testName = "".equals(testId) ? "test" : testId;

            try {
                final TestContainer<TestContext> test = tests.get(testId);
                if (test == null) {
                    // we log a warning: it could be that it is a newly created machine from mama-monkey.
                    log.warning("Failed to process command: " + command + " no test with " +
                            "testId " + testId + " is found");
                    return;
                }

                final Method method = test.getClass().getMethod(methodName);
                CommandThread commandThread = new CommandThread(command, command.testId) {
                    @Override
                    public void doRun() throws Throwable {
                        log.info(format("%s Starting %s.%s() %s", DASHES, testName, methodName, DASHES));

                        try {
                            method.invoke(test);
                            log.info(format("%s Finished %s.%s() %s", DASHES, testName, methodName, DASHES));
                        } catch (InvocationTargetException e) {
                            log.severe(format("%s Failed %s.%s() %s", DASHES, testName, methodName, DASHES));
                            throw e.getCause();
                        } finally {
                            if ("localTeardown".equals(methodName)) {
                                tests.remove(testId);
                            }
                        }
                    }
                };
                commandThread.start();
            } catch (Exception e) {
                log.severe(format("Failed to execute test.%s()", methodName), e);
                throw e;
            }
        }

        private void process(InitCommand command) throws Throwable {
            try {
                TestCase testCase = command.testCase;
                String testId = testCase.getId();
                if (tests.containsKey(testId)) {
                    throw new IllegalStateException("Can't init testcase: " + command + ", another test with [" + testId +
                            "] testId already exists");
                }

                log.info(format("%s Initializing test %s %s\n%s", DASHES, testId, testCase, DASHES));

                String clazzName = testCase.getClassname();
                Object testObject = InitCommand.class.getClassLoader().loadClass(clazzName).newInstance();
                bindProperties(testObject, testCase);
                ProbesConfiguration probesConfiguration = parseProbeConfiguration(testCase);


                TestContextImpl testContext = new TestContextImpl(testCase.id);
                TestContainer<TestContext> testContainer = new TestContainer<TestContext>(testObject, testContext, probesConfiguration);
                tests.put(testContext.getTestId(), testContainer);

                if (serverInstance != null) {
                    serverInstance.getUserContext().put(TestUtils.TEST_INSTANCE + ":" + testCase.id, testObject);
                }
            } catch (Throwable e) {
                log.severe("Failed to init Test", e);
                throw e;
            }
        }

        public void process(StopCommand command) throws Exception {
            try {
                String testId = command.testId;
                final String testName = "".equals(testId) ? "test" : testId;
                TestContainer<TestContext> test = tests.get(command.testId);
                if (test == null) {
                    log.warning("Can't stop test, test with id " + command.testId + " does not exist");
                    return;
                }

                log.info(format("%s %s.stop() %s", DASHES, testName, DASHES));
                test.getTestContext().stop();
            } catch (Exception e) {
                log.severe("Failed to execute test.stop", e);
                throw e;
            }
        }

        public boolean process(IsPhaseCompletedCommand command) throws Exception {
            return !commands.containsKey(command.testId);
        }
    }

    abstract class CommandThread extends Thread {

        private final Command command;
        private final String testId;

        public CommandThread(Command command, String testId) {
            this.command = command;
            this.testId = testId;
        }

        public abstract void doRun() throws Throwable;

        @Override
        public final void run() {
            try {
                commands.put(testId, command);
                doRun();
            } catch (Throwable t) {
                ExceptionReporter.report(testId, t);
            } finally {
                commands.remove(testId);
            }
        }
    }

    class TestContextImpl implements TestContext {
        private final String testId;
        volatile boolean stopped = false;

        TestContextImpl(String testId) {
            this.testId = testId;
        }

        @Override
        public HazelcastInstance getTargetInstance() {
            if (clientInstance != null) {
                return clientInstance;
            } else {
                return serverInstance;
            }
        }

        @Override
        public String getTestId() {
            return testId;
        }

        @Override
        public boolean isStopped() {
            return stopped;
        }

        @Override
        public void stop() {
            stopped = true;
        }
    }

    class PerformanceMonitorThread extends Thread {
	    private final File performanceFile = new File("performance.txt");
	    private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

        private long oldCount;
        private long oldTimeMillis = System.currentTimeMillis();

        public PerformanceMonitorThread() {
            super("PerformanceMonitorThread");
            setDaemon(true);

	        Utils.appendText("Timestamp                      Ops (sum)     Ops/s (interval)\n", performanceFile);
	        Utils.appendText("-------------------------------------------------------------\n", performanceFile);
        }

        @Override
        public void run() {
            for (; ; ) {
                try {
                    Thread.sleep(5000);
                    singleRun();
                } catch (Throwable t) {
                    log.severe("Failed to run performance monitor", t);
                }
            }
        }

        private void singleRun() {
            long currentCount = getCount();
            long delta = currentCount - oldCount;

            long currentTimeMs = System.currentTimeMillis();
            long durationMs = currentTimeMs - oldTimeMillis;

            double performance = (delta * 1000d) / durationMs;

            oldCount = currentCount;
            oldTimeMillis = currentTimeMs;

            Utils.appendText(format("[%s] %s ops %s ops/s\n",
				            simpleDateFormat.format(new Date()),
				            Utils.formatLong(currentCount, 14),
				            Utils.formatDouble(performance, 14)
                ), performanceFile
            );
        }

        private long getCount() {
            long operationCount = 0;
            for (TestContainer container : tests.values()) {
                try {
                    operationCount += container.getOperationCount();
                } catch (Throwable ignored) {
                }
            }

            return operationCount;
        }
    }
}