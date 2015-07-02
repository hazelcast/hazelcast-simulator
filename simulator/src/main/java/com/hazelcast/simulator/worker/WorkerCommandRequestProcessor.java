package com.hazelcast.simulator.worker;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.common.messaging.Message;
import com.hazelcast.simulator.probes.probes.ProbesConfiguration;
import com.hazelcast.simulator.probes.probes.Result;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.worker.commands.Command;
import com.hazelcast.simulator.worker.commands.CommandRequest;
import com.hazelcast.simulator.worker.commands.CommandResponse;
import com.hazelcast.simulator.worker.commands.GenericCommand;
import com.hazelcast.simulator.worker.commands.GetBenchmarkResultsCommand;
import com.hazelcast.simulator.worker.commands.GetOperationCountCommand;
import com.hazelcast.simulator.worker.commands.GetStackTraceCommand;
import com.hazelcast.simulator.worker.commands.InitCommand;
import com.hazelcast.simulator.worker.commands.IsPhaseCompletedCommand;
import com.hazelcast.simulator.worker.commands.MessageCommand;
import com.hazelcast.simulator.worker.commands.RunCommand;
import com.hazelcast.simulator.worker.commands.StopCommand;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.utils.FileUtils.isValidFileName;
import static com.hazelcast.simulator.utils.PropertyBindingSupport.bindProperties;
import static com.hazelcast.simulator.utils.PropertyBindingSupport.parseProbeConfiguration;
import static com.hazelcast.simulator.utils.TestUtils.getUserContextKeyFromTestId;
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

    private final ConcurrentMap<String, TestPhase> testPhases = new ConcurrentHashMap<String, TestPhase>();

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

    private final class WorkerCommandRequestProcessorThread extends Thread {

        private volatile boolean running = true;

        private WorkerCommandRequestProcessorThread() {
            super("WorkerCommandRequestProcessorThread");
        }

        @Override
        public void run() {
            while (running) {
                try {
                    CommandRequest request = requestQueue.take();
                    if (request != null) {
                        doProcess(request.id, request.task);
                    }
                } catch (Throwable e) {
                    ExceptionReporter.report(null, e);
                }
            }
        }

        private void doProcess(long id, Command command) throws Exception {
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
                } else if (command instanceof GetStackTraceCommand) {
                    result = process((GetStackTraceCommand) command);
                } else {
                    throw new RuntimeException("Unsupported command: " + command.getClass().getSimpleName());
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

        private boolean process(IsPhaseCompletedCommand command) {
            TestPhase testPhase = testPhases.get(command.testId);
            if (testPhase == null) {
                return true;
            }
            if (testPhase == command.testPhase) {
                return false;
            }
            throw new IllegalStateException("IsPhaseCompletedCommand(" + command.testId + ") checked phase " + command.testPhase
                    + " but test is in phase " + testPhase);
        }

        private void process(InitCommand command) throws Exception {
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

                LOGGER.info(format("%s Initializing test %s %s%n%s", DASHES, testId, DASHES, testCase));

                Object testInstance = InitCommand.class.getClassLoader().loadClass(testCase.getClassname()).newInstance();
                bindProperties(testInstance, testCase, TestContainer.OPTIONAL_TEST_PROPERTIES);
                TestContextImpl testContext = new TestContextImpl(testId, getHazelcastInstance());
                ProbesConfiguration probesConfiguration = parseProbeConfiguration(testCase);

                tests.put(testId, new TestContainer<TestContext>(testInstance, testContext, probesConfiguration, testCase));
                testsPending.incrementAndGet();

                if (serverInstance != null) {
                    serverInstance.getUserContext().put(getUserContextKeyFromTestId(testId), testInstance);
                }
            } catch (Exception e) {
                throw e;
            } catch (Throwable e) {
                LOGGER.fatal("Failed to init test", e);
                throw (Error) e;
            }
        }

        private void process(RunCommand command) throws Exception {
            if (workerPerformanceMonitor.start()) {
                LOGGER.info(format("%s Starting performance monitoring %s", DASHES, DASHES));
            }

            final String testId = command.testId;
            final String testName = "".equals(testId) ? "test" : testId;

            try {
                final TestContainer<TestContext> test = tests.get(testId);
                if (test == null) {
                    LOGGER.warn(format("Failed to process command %s (no test with testId %s is found)", command, testId));
                    return;
                }

                if (command.passiveMembers && clientInstance == null) {
                    LOGGER.info(format("%s Skipping run of %s (member is passive) %s", DASHES, testName, DASHES));
                    return;
                }

                CommandThread commandThread = new CommandThread(testId, TestPhase.RUN) {
                    @Override
                    public void doRun() throws Exception {
                        try {
                            LOGGER.info(format("%s Starting run of %s %s", DASHES, testName, DASHES));
                            test.invoke(TestPhase.RUN);
                            LOGGER.info(format("%s Completed run of %s %s", DASHES, testName, DASHES));
                        } catch (InvocationTargetException e) {
                            LOGGER.fatal(format("%s Failed to execute run of %s %s", DASHES, testName, DASHES), e.getCause());
                            if (e.getCause() instanceof Error) {
                                throw (Error) e.getCause();
                            }
                            if (e.getCause() instanceof Exception) {
                                throw (Exception) e.getCause();
                            }
                            throw new RuntimeException(format("Failed to execute run of %s", testName), e.getCause());
                        }

                        // stop performance monitor if all tests have completed their run phase
                        if (testsCompleted.incrementAndGet() == testsPending.get()) {
                            LOGGER.info(format("%s Stopping performance monitoring %s", DASHES, DASHES));
                            workerPerformanceMonitor.stop();
                        }
                    }
                };
                commandThread.start();
            } catch (Exception e) {
                LOGGER.fatal("Failed to start test " + testName, e.getCause());
                throw e;
            }
        }

        private void process(StopCommand command) throws Exception {
            String testId = command.testId;
            String testName = "".equals(testId) ? "test" : testId;
            try {
                TestContainer<TestContext> test = tests.get(testId);
                if (test == null) {
                    LOGGER.warn("Can't stop test, found no test with id " + testId);
                    return;
                }

                LOGGER.info(format("%s Stopping %s %s", DASHES, testName, DASHES));
                test.getTestContext().stop();
            } catch (Exception e) {
                LOGGER.fatal("Failed to stop test " + testName, e);
                throw e;
            }
        }

        private void process(GenericCommand command) throws Exception {
            final String testId = command.testId;
            final String testName = "".equals(testId) ? "test" : testId;
            final TestPhase testPhase = command.testPhase;

            try {
                final TestContainer<TestContext> test = tests.get(testId);
                if (test == null) {
                    // we log a warning: it could be that it's a newly created machine from mama-monkey
                    LOGGER.warn("Failed to process command: " + command + ", found no test with " + "testId " + testId);
                    return;
                }

                CommandThread commandThread = new CommandThread(command.testId, testPhase) {
                    @Override
                    public void doRun() throws Exception {
                        try {
                            LOGGER.info(format("%s Starting %s of %s %s", DASHES, testPhase.desc(), testName, DASHES));
                            test.invoke(testPhase);
                            LOGGER.info(format("%s Finished %s of %s %s", DASHES, testPhase.desc(), testName, DASHES));
                        } catch (InvocationTargetException e) {
                            LOGGER.fatal(format("%s Failed %s of %s %s", DASHES, testPhase.desc(), testName, DASHES));
                            if (e.getCause() instanceof Error) {
                                throw (Error) e.getCause();
                            }
                            if (e.getCause() instanceof Exception) {
                                throw (Exception) e.getCause();
                            }
                            throw new RuntimeException(format("Failed to execute %s of %s", testPhase.desc(), testName),
                                    e.getCause());
                        } finally {
                            if (testPhase == TestPhase.LOCAL_TEARDOWN) {
                                tests.remove(testId);
                            }
                        }
                    }
                };
                commandThread.start();
            } catch (Exception e) {
                LOGGER.fatal(format("Failed to execute %s of %s", testPhase.desc(), testName), e);
                throw e;
            }
        }

        private void process(MessageCommand command) {
            Message message = command.getMessage();
            workerMessageProcessor.submit(message);
        }

        @SuppressWarnings("unused")
        private Long process(GetOperationCountCommand command) throws Exception {
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

        private List<String> process(GetStackTraceCommand command) throws Exception {
            String testId = command.testId;
            String testName = "".equals(testId) ? "test" : testId;

            TestContainer<TestContext> test = tests.get(testId);
            if (test == null) {
                LOGGER.warn("Can't get stacktraces, found no test with id " + testId);
                return null;
            }

            LOGGER.info(String.format("%s %s.getStackTraces() %s", DASHES, testName, DASHES));
            return test.getStackTraces();
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

        private final String testId;

        public CommandThread(String testId, TestPhase testPhase) {
            this.testId = testId;

            TestPhase runningPhase = testPhases.putIfAbsent(testId, testPhase);
            if (runningPhase != null) {
                throw new IllegalStateException("Tried to start " + testPhase + " for test " + testId
                        + ", but " + runningPhase + " is still running!");
            }
        }

        @Override
        public final void run() {
            try {
                doRun();
            } catch (Throwable t) {
                ExceptionReporter.report(testId, t);
            } finally {
                testPhases.remove(testId);
            }
        }

        public abstract void doRun() throws Exception;
    }
}
