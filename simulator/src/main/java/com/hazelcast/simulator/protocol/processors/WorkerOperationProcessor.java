package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.exception.ExceptionLogger;
import com.hazelcast.simulator.protocol.operation.CreateTestOperation;
import com.hazelcast.simulator.protocol.operation.IsPhaseCompletedOperation;
import com.hazelcast.simulator.protocol.operation.OperationType;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.operation.StartTestOperation;
import com.hazelcast.simulator.protocol.operation.StartTestPhaseOperation;
import com.hazelcast.simulator.protocol.operation.StopTestOperation;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.worker.TestContainer;
import com.hazelcast.simulator.worker.TestContextImpl;
import com.hazelcast.simulator.worker.Worker;
import com.hazelcast.simulator.worker.WorkerType;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.TEST_PHASE_IS_COMPLETED;
import static com.hazelcast.simulator.protocol.core.ResponseType.TEST_PHASE_IS_RUNNING;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static com.hazelcast.simulator.utils.FileUtils.isValidFileName;
import static com.hazelcast.simulator.utils.PropertyBindingSupport.bindProperties;
import static com.hazelcast.simulator.utils.TestUtils.getUserContextKeyFromTestId;
import static java.lang.String.format;

/**
 * An {@link OperationProcessor} implementation to process {@link SimulatorOperation} instances on a Simulator Worker.
 */
public class WorkerOperationProcessor extends OperationProcessor {

    private static final String DASHES = "---------------------------";
    private static final Logger LOGGER = Logger.getLogger(WorkerOperationProcessor.class);

    private final AtomicInteger testsPending = new AtomicInteger(0);
    private final AtomicInteger testsCompleted = new AtomicInteger(0);

    private final ConcurrentMap<String, TestContainer> tests = new ConcurrentHashMap<String, TestContainer>();
    private final ConcurrentMap<String, TestPhase> testPhases = new ConcurrentHashMap<String, TestPhase>();

    private final ExceptionLogger exceptionLogger;

    private final WorkerType type;
    private final HazelcastInstance hazelcastInstance;
    private final Worker worker;

    public WorkerOperationProcessor(ExceptionLogger exceptionLogger, WorkerType type, HazelcastInstance hazelcastInstance,
                                    Worker worker) {
        super(exceptionLogger);
        this.exceptionLogger = exceptionLogger;

        this.type = type;
        this.hazelcastInstance = hazelcastInstance;
        this.worker = worker;
    }

    public Collection<TestContainer> getTests() {
        return tests.values();
    }

    @Override
    protected ResponseType processOperation(OperationType operationType, SimulatorOperation operation) throws Exception {
        switch (operationType) {
            case TERMINATE_WORKERS:
                processTerminateWorkers();
                break;
            case CREATE_TEST:
                processCreateTest((CreateTestOperation) operation);
                break;
            case IS_PHASE_COMPLETED:
                return processIsPhaseCompleted((IsPhaseCompletedOperation) operation);
            case START_TEST_PHASE:
                processStartTestPhase((StartTestPhaseOperation) operation);
                break;
            case START_TEST:
                processStartTest((StartTestOperation) operation);
                break;
            case STOP_TEST:
                processStopTest((StopTestOperation) operation);
                break;
            default:
                return UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
        }
        return SUCCESS;
    }

    private void processTerminateWorkers() {
        worker.shutdown();
    }

    private void processCreateTest(CreateTestOperation operation) throws Exception {
        TestCase testCase = operation.getTestCase();
        String testId = testCase.getId();
        if (tests.containsKey(testId)) {
            throw new IllegalStateException(format("Can't init TestCase: %s, another test with testId [%s] already exists",
                    operation, testId));
        }
        if (!testId.isEmpty() && !isValidFileName(testId)) {
            throw new IllegalArgumentException(format("Can't init TestCase: %s, testId [%s] is an invalid filename",
                    operation, testId));
        }

        LOGGER.info(format("%s Initializing test %s %s%n%s", DASHES, testId, DASHES, testCase));

        Object testInstance = CreateTestOperation.class.getClassLoader().loadClass(testCase.getClassname()).newInstance();
        bindProperties(testInstance, testCase, TestContainer.OPTIONAL_TEST_PROPERTIES);
        TestContextImpl testContext = new TestContextImpl(testId, hazelcastInstance);

        tests.put(testId, new TestContainer(testInstance, testContext, testCase));
        testsPending.incrementAndGet();

        if (type == WorkerType.MEMBER) {
            hazelcastInstance.getUserContext().put(getUserContextKeyFromTestId(testId), testInstance);
        }
    }

    private ResponseType processIsPhaseCompleted(IsPhaseCompletedOperation operation) {
        TestPhase testPhase = testPhases.get(operation.getTestId());
        if (testPhase == null) {
            return TEST_PHASE_IS_COMPLETED;
        }
        if (testPhase == operation.getTestPhase()) {
            return TEST_PHASE_IS_RUNNING;
        }
        throw new IllegalStateException(format("IsPhaseCompletedCommand(%s) checked phase %s but test is in phase %s",
                operation.getTestId(), operation.getTestPhase(), testPhase));
    }

    private void processStartTestPhase(StartTestPhaseOperation operation) throws Exception {
        final String testId = operation.getTestId();
        final String testName = "".equals(testId) ? "test" : testId;
        final TestPhase testPhase = operation.getTestPhase();

        try {
            final TestContainer test = tests.get(testId);
            if (test == null) {
                // we log a warning: it could be that it's a newly created machine from mama-monkey
                LOGGER.warn(format("Failed to process operation %s, found no test with testId %s", operation, testId));
                return;
            }

            OperationThread operationThread = new OperationThread(testId, testPhase) {
                @Override
                public void doRun() throws Exception {
                    try {
                        LOGGER.info(format("%s Starting %s of %s %s", DASHES, testPhase.desc(), testName, DASHES));
                        test.invoke(testPhase);
                        LOGGER.info(format("%s Finished %s of %s %s", DASHES, testPhase.desc(), testName, DASHES));
                    } finally {
                        if (testPhase == TestPhase.LOCAL_TEARDOWN) {
                            tests.remove(testId);
                        }
                    }
                }
            };
            operationThread.start();
        } catch (Exception e) {
            LOGGER.fatal(format("Failed to execute %s of %s", testPhase.desc(), testName), e);
            throw e;
        }
    }

    private void processStartTest(StartTestOperation operation) {
        if (worker.startPerformanceMonitor()) {
            LOGGER.info(format("%s Starting performance monitoring %s", DASHES, DASHES));
        }

        final String testId = operation.getTestId();
        final String testName = "".equals(testId) ? "test" : testId;

        final TestContainer test = tests.get(testId);
        if (test == null) {
            LOGGER.warn(format("Failed to process operation %s (no test with testId %s is found)", operation, testId));
            return;
        }

        if (operation.isPassiveMember() && type == WorkerType.MEMBER) {
            LOGGER.info(format("%s Skipping run of %s (member is passive) %s", DASHES, testName, DASHES));
            return;
        }

        OperationThread operationThread = new OperationThread(testId, TestPhase.RUN) {
            @Override
            public void doRun() throws Exception {
                LOGGER.info(format("%s Starting run of %s %s", DASHES, testName, DASHES));
                test.invoke(TestPhase.RUN);
                LOGGER.info(format("%s Completed run of %s %s", DASHES, testName, DASHES));

                // stop performance monitor if all tests have completed their run phase
                if (testsCompleted.incrementAndGet() == testsPending.get()) {
                    LOGGER.info(format("%s Stopping performance monitoring %s", DASHES, DASHES));
                    worker.shutdownPerformanceMonitor();
                }
            }
        };
        operationThread.start();
    }

    private void processStopTest(StopTestOperation operation) {
        String testId = operation.getTestId();
        String testName = "".equals(testId) ? "test" : testId;

        TestContainer test = tests.get(testId);
        if (test == null) {
            LOGGER.warn("Can't stop test, found no test with id " + testId);
            return;
        }

        LOGGER.info(format("%s Stopping %s %s", DASHES, testName, DASHES));
        test.getTestContext().stop();
    }

    private abstract class OperationThread extends Thread {

        private final String testId;

        public OperationThread(String testId, TestPhase testPhase) {
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
                LOGGER.error("Error while executing test phase", t);
                exceptionLogger.log(t, testId);
            } finally {
                testPhases.remove(testId);
            }
        }

        public abstract void doRun() throws Exception;
    }
}
