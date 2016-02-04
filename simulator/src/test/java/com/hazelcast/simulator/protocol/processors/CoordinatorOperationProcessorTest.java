package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.simulator.coordinator.FailureContainer;
import com.hazelcast.simulator.coordinator.FailureListener;
import com.hazelcast.simulator.coordinator.PerformanceStateContainer;
import com.hazelcast.simulator.coordinator.TestHistogramContainer;
import com.hazelcast.simulator.coordinator.TestPhaseListener;
import com.hazelcast.simulator.coordinator.TestPhaseListenerContainer;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.exception.LocalExceptionLogger;
import com.hazelcast.simulator.protocol.operation.ExceptionOperation;
import com.hazelcast.simulator.protocol.operation.FailureOperation;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.PerformanceStateOperation;
import com.hazelcast.simulator.protocol.operation.PhaseCompletedOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.operation.TestHistogramOperation;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.test.FailureType;
import com.hazelcast.simulator.test.TestException;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.worker.performance.PerformanceState;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.TestEnvironmentUtils.resetLogLevel;
import static com.hazelcast.simulator.TestEnvironmentUtils.setLogLevel;
import static com.hazelcast.simulator.coordinator.PerformanceStateContainer.LATENCY_FORMAT_LENGTH;
import static com.hazelcast.simulator.coordinator.PerformanceStateContainer.OPERATION_COUNT_FORMAT_LENGTH;
import static com.hazelcast.simulator.coordinator.PerformanceStateContainer.THROUGHPUT_FORMAT_LENGTH;
import static com.hazelcast.simulator.protocol.core.AddressLevel.TEST;
import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;
import static com.hazelcast.simulator.protocol.core.ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.protocol.operation.OperationType.getOperationType;
import static com.hazelcast.simulator.test.FailureType.WORKER_EXCEPTION;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FormatUtils.formatDouble;
import static com.hazelcast.simulator.utils.FormatUtils.formatLong;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CoordinatorOperationProcessorTest implements FailureListener {

    private BlockingQueue<FailureOperation> failureOperations = new LinkedBlockingQueue<FailureOperation>();

    private SimulatorAddress workerAddress;

    private LocalExceptionLogger exceptionLogger;
    private TestPhaseListenerContainer testPhaseListenerContainer;
    private PerformanceStateContainer performanceStateContainer;
    private TestHistogramContainer testHistogramContainer;
    private FailureContainer failureContainer;

    private CoordinatorOperationProcessor processor;

    @BeforeClass
    public static void setUpEnvironment() {
        setLogLevel(Level.TRACE);
    }

    @AfterClass
    public static void resetEnvironment() {
        resetLogLevel();
    }

    @Before
    public void setUp() {
        workerAddress = new SimulatorAddress(WORKER, 1, 1, 0);

        ComponentRegistry componentRegistry = new ComponentRegistry();

        exceptionLogger = new LocalExceptionLogger();
        testPhaseListenerContainer = new TestPhaseListenerContainer();
        performanceStateContainer = new PerformanceStateContainer();
        testHistogramContainer = new TestHistogramContainer(performanceStateContainer);
        failureContainer = new FailureContainer("CoordinatorOperationProcessorTest", componentRegistry);

        processor = new CoordinatorOperationProcessor(exceptionLogger, failureContainer, testPhaseListenerContainer,
                performanceStateContainer, testHistogramContainer);
    }

    @After
    public void tearDown() {
        deleteQuiet("failures-CoordinatorOperationProcessorTest.txt");
    }

    @Override
    public void onFailure(FailureOperation operation) {
        failureOperations.add(operation);
    }

    @Test
    public void testProcessOperation_unsupportedOperation() throws Exception {
        SimulatorOperation operation = new IntegrationTestOperation();
        ResponseType responseType = processor.processOperation(getOperationType(operation), operation, COORDINATOR);

        assertEquals(UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR, responseType);
    }

    @Test
    public void processException() {
        TestException exception = new TestException("expected exception");
        ExceptionOperation operation = new ExceptionOperation(WORKER_EXCEPTION.name(), "C_A1_W1", "FailingTest", exception);

        ResponseType responseType = processor.process(operation, workerAddress);

        assertEquals(SUCCESS, responseType);
        assertEquals(1, exceptionLogger.getExceptionCount());
    }

    @Test
    public void processFailureOperation() {
        failureContainer.addListener(this);

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

        testPhaseListenerContainer.addListener(1, new TestPhaseListener() {
            @Override
            public void completed(TestPhase testPhase) {
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
    public void processPerformanceState() {
        PerformanceStateOperation operation = new PerformanceStateOperation();
        operation.addPerformanceState("testId", new PerformanceState(1000, 50.0, 1234.56, 33.0d, 23, 42));
        performanceStateContainer.init("testId");

        ResponseType responseType = processor.process(operation, workerAddress);
        assertEquals(SUCCESS, responseType);

        String performanceNumbers = performanceStateContainer.getPerformanceNumbers("testId");
        assertTrue(performanceNumbers.contains(formatLong(1000, OPERATION_COUNT_FORMAT_LENGTH)));
        assertTrue(performanceNumbers.contains(formatDouble(50, THROUGHPUT_FORMAT_LENGTH)));
        assertTrue(performanceNumbers.contains(formatLong(23, LATENCY_FORMAT_LENGTH)));
        assertTrue(performanceNumbers.contains(formatLong(33, LATENCY_FORMAT_LENGTH)));
        assertTrue(performanceNumbers.contains(formatLong(42, LATENCY_FORMAT_LENGTH)));
    }

    @Test
    public void processTestHistogram() {
        Map<String, String> probeHistograms = new HashMap<String, String>();
        probeHistograms.put("probe1", "histogram1");
        probeHistograms.put("probe2", "histogram2");
        TestHistogramOperation operation = new TestHistogramOperation("testId", probeHistograms);

        ResponseType responseType = processor.process(operation, workerAddress);
        assertEquals(SUCCESS, responseType);

        ConcurrentMap<String, Map<String, String>> testHistograms = testHistogramContainer.getTestHistograms(workerAddress);
        assertNotNull(testHistograms);
        assertEquals(1, testHistograms.size());

        Map<String, String> actualProbeHistograms = testHistograms.get("testId");
        assertNotNull(actualProbeHistograms);
        assertEquals("histogram1", actualProbeHistograms.get("probe1"));
        assertEquals("histogram2", actualProbeHistograms.get("probe2"));
    }

    private static void assertExceptionClassInFailure(FailureOperation failure, Class<? extends Throwable> failureClass) {
        assertTrue(format("Expected cause to start with %s, but was %s", failureClass.getCanonicalName(), failure.getCause()),
                failure.getCause().startsWith(failureClass.getCanonicalName()));
    }
}
