package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.simulator.common.FailureType;
import com.hazelcast.simulator.coordinator.FailureCollector;
import com.hazelcast.simulator.coordinator.FailureListener;
import com.hazelcast.simulator.coordinator.PerformanceStatsCollector;
import com.hazelcast.simulator.coordinator.TestPhaseListener;
import com.hazelcast.simulator.coordinator.TestPhaseListeners;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.FailureOperation;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.PerformanceStatsOperation;
import com.hazelcast.simulator.protocol.operation.PhaseCompletedOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.test.TestException;
import com.hazelcast.simulator.testcontainer.TestPhase;
import com.hazelcast.simulator.utils.TestUtils;
import com.hazelcast.simulator.worker.performance.PerformanceStats;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.coordinator.PerformanceStatsCollector.LATENCY_FORMAT_LENGTH;
import static com.hazelcast.simulator.coordinator.PerformanceStatsCollector.OPERATION_COUNT_FORMAT_LENGTH;
import static com.hazelcast.simulator.coordinator.PerformanceStatsCollector.THROUGHPUT_FORMAT_LENGTH;
import static com.hazelcast.simulator.protocol.core.AddressLevel.TEST;
import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;
import static com.hazelcast.simulator.protocol.core.ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.protocol.operation.OperationType.getOperationType;
import static com.hazelcast.simulator.utils.FormatUtils.formatDouble;
import static com.hazelcast.simulator.utils.FormatUtils.formatLong;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CoordinatorOperationProcessorTest implements FailureListener {

    private BlockingQueue<FailureOperation> failureOperations = new LinkedBlockingQueue<FailureOperation>();

    private SimulatorAddress workerAddress;

    private TestPhaseListeners testPhaseListeners;
    private PerformanceStatsCollector performanceStatsCollector;
    private FailureCollector failureCollector;

    private CoordinatorOperationProcessor processor;
    private File outputDirectory;

    @Before
    public void setUp() {
        workerAddress = new SimulatorAddress(WORKER, 1, 1, 0);

        testPhaseListeners = new TestPhaseListeners();
        performanceStatsCollector = new PerformanceStatsCollector();

        outputDirectory = TestUtils.createTmpDirectory();
        failureCollector = new FailureCollector(outputDirectory);

        processor = new CoordinatorOperationProcessor(null, failureCollector, testPhaseListeners,
                performanceStatsCollector);
    }

    @Override
    public void onFailure(FailureOperation failure, boolean isFinishedFailure, boolean isCritical) {
        failureOperations.add(failure);
    }

    @Test
    public void testProcessOperation_unsupportedOperation() throws Exception {
        SimulatorOperation operation = new IntegrationTestOperation();
        ResponseType responseType = processor.processOperation(getOperationType(operation), operation, COORDINATOR);

        assertEquals(UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR, responseType);
    }

//    @Test
//    public void processException() {
//        TestException exception = new TestException("expected exception");
//        ExceptionOperation operation = new ExceptionOperation(WORKER_EXCEPTION.name(), "C_A1_W1", "FailingTest", exception);
//
//        ResponseType responseType = processor.process(operation, workerAddress);
//
//        assertEquals(SUCCESS, responseType);
//        //assertEquals(1, exceptionLogger.getExceptionCount());
//    }

    @Test
    public void processFailureOperation() {
        failureCollector.addListener(this);

        TestException exception = new TestException("expected exception");
        FailureOperation operation = new FailureOperation("CoordinatorOperationProcessorTest", FailureType.WORKER_OOM,
                workerAddress, workerAddress.getParent().toString(), exception);
        ResponseType responseType = processor.process(operation, workerAddress);

        assertEquals(SUCCESS, responseType);
        assertEquals(1, failureOperations.size());

        FailureOperation failure = failureOperations.poll();
        assertNull(failure.getTestId());
        assertExceptionClassInFailure(failure, TestException.class);
    }

    @Test
    public void processPhaseCompletion() {
        final AtomicInteger phaseCompleted = new AtomicInteger();

        PhaseCompletedOperation operation = new PhaseCompletedOperation(TestPhase.LOCAL_TEARDOWN);

        testPhaseListeners.addListener(1, new TestPhaseListener() {
            @Override
            public void completed(TestPhase testPhase, SimulatorAddress workerAddress) {
                if (testPhase.equals(TestPhase.LOCAL_TEARDOWN)) {
                    phaseCompleted.incrementAndGet();
                }
            }
        });

        ResponseType responseType = processor.process(operation, new SimulatorAddress(TEST, 1, 1, 1));
        assertEquals(SUCCESS, responseType);
        assertEquals(1, phaseCompleted.get());

        responseType = processor.process(operation, new SimulatorAddress(TEST, 1, 2, 1));
        assertEquals(SUCCESS, responseType);
        assertEquals(2, phaseCompleted.get());
    }

    @Test
    public void processPhaseCompletion_withOperationFromWorker() {
        PhaseCompletedOperation operation = new PhaseCompletedOperation(TestPhase.LOCAL_TEARDOWN);
        ResponseType responseType = processor.process(operation, workerAddress);

        assertEquals(EXCEPTION_DURING_OPERATION_EXECUTION, responseType);
    }

    @Test
    public void processPerformanceStats() {
        PerformanceStatsOperation operation = new PerformanceStatsOperation();
        operation.addPerformanceStats("testId", new PerformanceStats(1000, 50.0, 1234.56, 33000.0d, 23000, 42000));

        ResponseType responseType = processor.process(operation, workerAddress);
        assertEquals(SUCCESS, responseType);

        String performanceNumbers = performanceStatsCollector.formatPerformanceNumbers("testId");
        assertTrue(performanceNumbers.contains(formatLong(1000, OPERATION_COUNT_FORMAT_LENGTH)));
        assertTrue(performanceNumbers.contains(formatDouble(50, THROUGHPUT_FORMAT_LENGTH)));
        assertTrue(performanceNumbers.contains(formatLong(23, LATENCY_FORMAT_LENGTH)));
        assertTrue(performanceNumbers.contains(formatLong(33, LATENCY_FORMAT_LENGTH)));
        assertTrue(performanceNumbers.contains(formatLong(42, LATENCY_FORMAT_LENGTH)));
    }

    private static void assertExceptionClassInFailure(FailureOperation failure, Class<? extends Throwable> failureClass) {
        assertTrue(format("Expected cause to start with %s, but was %s", failureClass.getCanonicalName(), failure.getCause()),
                failure.getCause().startsWith(failureClass.getCanonicalName()));
    }
}
