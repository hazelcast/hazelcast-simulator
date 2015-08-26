package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.probes.probes.ProbesConfiguration;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.operation.CreateTestOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.worker.TestContainer;
import com.hazelcast.simulator.worker.TestContextImpl;
import org.apache.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static com.hazelcast.simulator.utils.FileUtils.isValidFileName;
import static com.hazelcast.simulator.utils.PropertyBindingSupport.bindProperties;
import static com.hazelcast.simulator.utils.PropertyBindingSupport.parseProbeConfiguration;
import static com.hazelcast.simulator.utils.TestUtils.getUserContextKeyFromTestId;
import static java.lang.String.format;

/**
 * An {@link OperationProcessor} implementation to process {@link SimulatorOperation} instances on a Simulator Worker.
 */
public class WorkerOperationProcessor extends OperationProcessor {

    private static final String DASHES = "---------------------------";
    private static final Logger LOGGER = Logger.getLogger(WorkerOperationProcessor.class);

    private final AtomicInteger testsPending = new AtomicInteger(0);
    //private final AtomicInteger testsCompleted = new AtomicInteger(0);

    private final ConcurrentMap<String, TestContainer<TestContext>> tests
            = new ConcurrentHashMap<String, TestContainer<TestContext>>();

    //private final ConcurrentMap<String, TestPhase> testPhases = new ConcurrentHashMap<String, TestPhase>();

    private final HazelcastInstance serverInstance;
    private final HazelcastInstance clientInstance;

    public WorkerOperationProcessor(HazelcastInstance serverInstance, HazelcastInstance clientInstance) {
        this.serverInstance = serverInstance;
        this.clientInstance = clientInstance;
    }

    @Override
    protected ResponseType processOperation(SimulatorOperation operation) throws Exception {
        switch (operation.getOperationType()) {
            case CREATE_TEST:
                processCreateTest((CreateTestOperation) operation);
                break;
            default:
                return UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
        }
        return SUCCESS;
    }

    private void processCreateTest(CreateTestOperation operation) throws Exception {
        TestCase testCase = operation.getTestCase();
        String testId = testCase.getId();
        if (tests.containsKey(testId)) {
            throw new IllegalStateException(
                    format("Can't init TestCase: %s, another test with testId [%s] already exists", operation, testId));
        }
        if (!testId.isEmpty() && !isValidFileName(testId)) {
            throw new IllegalArgumentException(
                    format("Can't init TestCase: %s, testId [%s] is an invalid filename", operation, testId));
        }

        LOGGER.info(format("%s Initializing test %s %s%n%s", DASHES, testId, DASHES, testCase));

        Object testInstance = CreateTestOperation.class.getClassLoader().loadClass(testCase.getClassname()).newInstance();
        bindProperties(testInstance, testCase, TestContainer.OPTIONAL_TEST_PROPERTIES);
        TestContextImpl testContext = new TestContextImpl(testId, getHazelcastInstance());
        ProbesConfiguration probesConfiguration = parseProbeConfiguration(testInstance, testCase);

        tests.put(testId, new TestContainer<TestContext>(testInstance, testContext, probesConfiguration, testCase));
        testsPending.incrementAndGet();

        if (serverInstance != null) {
            serverInstance.getUserContext().put(getUserContextKeyFromTestId(testId), testInstance);
        }
    }

    private HazelcastInstance getHazelcastInstance() {
        if (clientInstance != null) {
            return clientInstance;
        }
        return serverInstance;
    }
}
