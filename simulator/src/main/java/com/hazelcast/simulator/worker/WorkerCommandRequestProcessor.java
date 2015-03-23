package com.hazelcast.simulator.worker;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.common.messaging.Message;
import com.hazelcast.simulator.probes.probes.ProbesConfiguration;
import com.hazelcast.simulator.probes.probes.Result;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.utils.TestUtils;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.worker.commands.Command;
import com.hazelcast.simulator.worker.commands.CommandRequest;
import com.hazelcast.simulator.worker.commands.CommandResponse;
import com.hazelcast.simulator.worker.commands.GenericCommand;
import com.hazelcast.simulator.worker.commands.GetBenchmarkResultsCommand;
import com.hazelcast.simulator.worker.commands.GetOperationCountCommand;
import com.hazelcast.simulator.worker.commands.InitCommand;
import com.hazelcast.simulator.worker.commands.IsPhaseCompletedCommand;
import com.hazelcast.simulator.worker.commands.MessageCommand;
import com.hazelcast.simulator.worker.commands.RunCommand;
import com.hazelcast.simulator.worker.commands.StopCommand;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.utils.FileUtils.isValidFileName;
import static com.hazelcast.simulator.utils.PropertyBindingSupport.bindProperties;
import static com.hazelcast.simulator.utils.PropertyBindingSupport.parseProbeConfiguration;
import static java.lang.String.format;

/**
 * Processes {@link Command} instances on {@link MemberWorker} and {@link ClientWorker} instances.
 *
 * These commands e.g. init, start and stop tests, so this instance will eventually call the annotated methods of the tests.
 */
class WorkerCommandRequestProcessor {

    private static final String DASHES = "---------------------------";
    private static final Logger LOGGER = Logger.getLogger(WorkerCommandRequestProcessor.class);

    private final AtomicInteger testsPending = new AtomicInteger(0);
    private final AtomicInteger testsCompleted = new AtomicInteger(0);

    private final ConcurrentMap<String, TestContainer<TestContext>> tests
            = new ConcurrentHashMap<String, TestContainer<TestContext>>();

    private final ConcurrentMap<String, Command> commands = new ConcurrentHashMap<String, Command>();

    private final BlockingQueue<CommandRequest> requestQueue;
    private final BlockingQueue<CommandResponse> responseQueue;

    private final HazelcastInstance serverInstance;
    private final HazelcastInstance clientInstance;

    private final WorkerPerformanceMonitor workerPerformanceMonitor;
    private final WorkerMessageProcessor workerMessageProcessor;
    private final WorkerCommandRequestProcessorThread workerCommandRequestProcessorThread;

    public WorkerCommandRequestProcessor(BlockingQueue<CommandRequest> requestQueue, BlockingQueue<CommandResponse> responseQueue,
                                         HazelcastInstance serverInstance, HazelcastInstance clientInstance) {
        this.requestQueue = requestQueue;
        this.responseQueue = responseQueue;

        this.serverInstance = serverInstance;
        this.clientInstance = clientInstance;

        // will be started lazily on first test run
        workerPerformanceMonitor = new WorkerPerformanceMonitor(tests.values());

        workerMessageProcessor = new WorkerMessageProcessor(tests);
        workerMessageProcessor.setHazelcastServerInstance(serverInstance);
        workerMessageProcessor.setHazelcastClientInstance(clientInstance);

        workerCommandRequestProcessorThread = new WorkerCommandRequestProcessorThread();
        workerCommandRequestProcessorThread.start();
    }

    void shutdown() {
        workerCommandRequestProcessorThread.running = false;
    }

    private class WorkerCommandRequestProcessorThread extends Thread {
        private volatile boolean running = true;

        private WorkerCommandRequestProcessorThread() {
            super("WorkerCommandRequestProcessorThread");
        }

        @Override
        public void run() {
            while (running) {
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
                } else if (command instanceof MessageCommand) {
                    process((MessageCommand) command);
                } else if (command instanceof GetOperationCountCommand) {
                    result = process((GetOperationCountCommand) command);
                } else if (command instanceof GetBenchmarkResultsCommand) {
                    result = process((GetBenchmarkResultsCommand) command);
                } else {
                    throw new RuntimeException("Unhandled command: " + command.getClass());
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

        private boolean process(IsPhaseCompletedCommand command) throws Exception {
            return !commands.containsKey(command.testId);
        }

        private void process(InitCommand command) throws Throwable {
            try {
                TestCase testCase = command.testCase;
                String testId = testCase.getId();
                if (tests.containsKey(testId)) {
                    throw new IllegalStateException(
                            format("Can't init TestCase: %s, another test with testId [%s] already exists", command, testId));
                }
                if (!testId.isEmpty() && !isValidFileName(testId)) {
                    throw new IllegalArgumentException(
                            format("Can't init TestCase: %s, testId [%s] is an invalid filename", command, testId));
                }

                LOGGER.info(String.format("%s Initializing test %s %s%n%s", DASHES, testId, DASHES, testCase));

                Object testInstance = InitCommand.class.getClassLoader().loadClass(testCase.getClassname()).newInstance();
                bindProperties(testInstance, testCase, TestContainer.OPTIONAL_TEST_PROPERTIES);
                TestContextImpl testContext = new TestContextImpl(testCase.id, getHazelcastInstance());
                ProbesConfiguration probesConfiguration = parseProbeConfiguration(testCase);

                tests.put(testContext.getTestId(), new TestContainer<TestContext>(testInstance, testContext, probesConfiguration,
                        testCase));
                testsPending.incrementAndGet();

                if (serverInstance != null) {
                    serverInstance.getUserContext().put(TestUtils.TEST_INSTANCE + ":" + testCase.id, testInstance);
                }
            } catch (Throwable e) {
                LOGGER.fatal("Failed to init test", e);
                throw e;
            }
        }

        private void process(final RunCommand command) throws Exception {
            if (workerPerformanceMonitor.start()) {
                LOGGER.info(String.format("%s Starting performance monitoring %s", DASHES, DASHES));
            }

            try {
                final String testId = command.testId;
                final String testName = "".equals(testId) ? "test" : testId;

                final TestContainer<TestContext> test = tests.get(testId);
                if (test == null) {
                    LOGGER.warn(format("Failed to process command %s (no test with testId %s is found)", command, testId));
                    return;
                }

                CommandThread commandThread = new CommandThread(command, testId) {
                    @Override
                    public void doRun() throws Throwable {
                        boolean passive = command.clientOnly && clientInstance == null;

                        if (passive) {
                            LOGGER.info(String.format("%s Skipping %s.run() (member is passive) %s", DASHES, testName, DASHES));
                        } else {
                            LOGGER.info(String.format("%s Starting %s.run() %s", DASHES, testId, DASHES));

                            try {
                                test.run();
                                LOGGER.info(String.format("%s Completed %s.run() %s", DASHES, testName, DASHES));
                            } catch (InvocationTargetException e) {
                                LOGGER.fatal(String.format("%s Failed to execute %s.run() %s", DASHES, testName, DASHES),
                                        e.getCause());
                                throw e.getCause();
                            }

                            // stop performance monitor if all tests have completed their run phase
                            if (testsCompleted.incrementAndGet() == testsPending.get()) {
                                LOGGER.info(String.format("%s Stopping performance monitoring %s", DASHES, DASHES));
                                workerPerformanceMonitor.stop();
                            }
                        }
                    }
                };
                commandThread.start();
            } catch (Exception e) {
                LOGGER.fatal("Failed to start test", e);
                throw e;
            }
        }

        private void process(StopCommand command) throws Exception {
            try {
                String testId = command.testId;
                final String testName = "".equals(testId) ? "test" : testId;
                TestContainer<TestContext> test = tests.get(command.testId);
                if (test == null) {
                    LOGGER.warn("Can't stop test, test with id " + command.testId + " does not exist");
                    return;
                }

                LOGGER.info(String.format("%s %s.stop() %s", DASHES, testName, DASHES));
                test.getTestContext().stop();
            } catch (Exception e) {
                LOGGER.fatal("Failed to execute test.stop", e);
                throw e;
            }
        }

        private void process(final GenericCommand command) throws Throwable {
            final String methodName = command.methodName;
            final String testId = command.testId;
            final String testName = "".equals(testId) ? "test" : testId;

            try {
                final TestContainer<TestContext> test = tests.get(testId);
                if (test == null) {
                    // we log a warning: it could be that it is a newly created machine from mama-monkey.
                    LOGGER.warn("Failed to process command: " + command + " no test with " +
                            "testId " + testId + " is found");
                    return;
                }

                final Method method = test.getClass().getMethod(methodName);
                CommandThread commandThread = new CommandThread(command, command.testId) {
                    @Override
                    public void doRun() throws Throwable {
                        LOGGER.info(String.format("%s Starting %s.%s() %s", DASHES, testName, methodName, DASHES));

                        try {
                            method.invoke(test);
                            LOGGER.info(String.format("%s Finished %s.%s() %s", DASHES, testName, methodName, DASHES));
                        } catch (InvocationTargetException e) {
                            LOGGER.fatal(String.format("%s Failed %s.%s() %s", DASHES, testName, methodName, DASHES));
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
                LOGGER.fatal(format("Failed to execute test.%s()", methodName), e);
                throw e;
            }
        }

        private void process(MessageCommand command) {
            Message message = command.getMessage();
            workerMessageProcessor.submit(message);
        }

        @SuppressWarnings("unused")
        private Long process(GetOperationCountCommand command) throws Throwable {
            long result = 0;

            for (TestContainer testContainer : tests.values()) {
                long operationCount = testContainer.getOperationCount();
                if (operationCount > 0) {
                    result += operationCount;
                }
            }

            return result;
        }

        private Map<String, Result<?>> process(GetBenchmarkResultsCommand command) {
            String testId = command.getTestId();
            return tests.get(testId).getProbeResults();
        }

        private HazelcastInstance getHazelcastInstance() {
            if (clientInstance != null) {
                return clientInstance;
            } else {
                return serverInstance;
            }
        }
    }

    private abstract class CommandThread extends Thread {

        private final Command command;
        private final String testId;

        public CommandThread(Command command, String testId) {
            this.command = command;
            this.testId = testId;
        }

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

        public abstract void doRun() throws Throwable;
    }
}
