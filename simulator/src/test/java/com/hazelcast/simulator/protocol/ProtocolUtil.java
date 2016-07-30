package com.hazelcast.simulator.protocol;

import com.hazelcast.simulator.agent.Agent;
import com.hazelcast.simulator.agent.workerprocess.WorkerProcessManager;
import com.hazelcast.simulator.coordinator.FailureContainer;
import com.hazelcast.simulator.coordinator.HdrHistogramContainer;
import com.hazelcast.simulator.coordinator.PerformanceStateContainer;
import com.hazelcast.simulator.coordinator.TestPhaseListeners;
import com.hazelcast.simulator.protocol.connector.AgentConnector;
import com.hazelcast.simulator.protocol.connector.ClientConnector;
import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.connector.ServerConnector;
import com.hazelcast.simulator.protocol.connector.WorkerConnector;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.exception.ExceptionLogger;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.processors.TestOperationProcessor;
import com.hazelcast.simulator.test.FailureType;
import com.hazelcast.simulator.test.TestContainer;
import com.hazelcast.simulator.utils.TestUtils;
import com.hazelcast.simulator.utils.ThreadSpawner;
import com.hazelcast.simulator.worker.Worker;
import com.hazelcast.util.ExceptionUtil;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.simulator.TestEnvironmentUtils.deleteLogs;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.worker.WorkerType.MEMBER;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ProtocolUtil {

    static final SimulatorOperation DEFAULT_OPERATION = new IntegrationTestOperation();

    static final long DEFAULT_TEST_TIMEOUT_MILLIS = 10000;

    static final int AGENT_START_PORT = 10000 + new Random().nextInt(1000);
    private static final int WORKER_START_PORT = AGENT_START_PORT + 1000;

    private static final Logger LOGGER = Logger.getLogger(ProtocolUtil.class);

    private static final AddressLevel MIN_ADDRESS_LEVEL = AddressLevel.AGENT;
    private static final int MIN_ADDRESS_LEVEL_VALUE = MIN_ADDRESS_LEVEL.toInt();

    private static final Random RANDOM = new Random();

    private static final ExceptionLogger EXCEPTION_LOGGER = mock(ExceptionLogger.class);

    private static CoordinatorConnector coordinatorConnector;
    private static List<AgentConnector> agentConnectors = new ArrayList<AgentConnector>();
    private static List<WorkerConnector> workerConnectors = new ArrayList<WorkerConnector>();

    static void startSimulatorComponents(int numberOfAgents, int numberOfWorkers, int numberOfTests) {
        try {
            for (int agentIndex = 1; agentIndex <= numberOfAgents; agentIndex++) {
                int workerStartPort = WORKER_START_PORT + (100 * (agentIndex - 1));
                for (int workerIndex = 1; workerIndex <= numberOfWorkers; workerIndex++) {
                    workerConnectors.add(startWorker(workerIndex, agentIndex, workerStartPort + workerIndex, numberOfTests));
                }

                int agentPort = AGENT_START_PORT + agentIndex;
                agentConnectors.add(startAgent(agentIndex, agentPort, "127.0.0.1", workerStartPort, numberOfWorkers));
            }

            coordinatorConnector = startCoordinator("127.0.0.1", AGENT_START_PORT, numberOfAgents);
        } catch (Exception e) {
            LOGGER.error("Exception in ProtocolUtil.startSimulatorComponents()", e);
            throw ExceptionUtil.rethrow(e);
        }
    }

    static void stopSimulatorComponents() {
        ThreadSpawner spawner = new ThreadSpawner("shutdownSimulatorComponents", true);

        shutdownCoordinatorConnector();

        LOGGER.info("Shutdown of Agents...");
        shutdownServerConnectors(agentConnectors, spawner);

        LOGGER.info("Shutdown of Workers...");
        shutdownServerConnectors(workerConnectors, spawner);

        LOGGER.info("Waiting for shutdown threads...");
        spawner.awaitCompletion();

        deleteLogs();
        LOGGER.info("Shutdown complete!");
    }

    static void shutdownCoordinatorConnector() {
        LOGGER.info("Shutdown of Coordinator...");
        if (coordinatorConnector != null) {
            coordinatorConnector.shutdown();
            coordinatorConnector = null;
        }
    }

    private static <C extends ServerConnector> void shutdownServerConnectors(List<C> connectors, ThreadSpawner spawner) {
        for (final C connector : connectors) {
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    connector.shutdown();
                }
            });
        }
        connectors.clear();
    }

    private static WorkerConnector startWorker(int addressIndex, int parentAddressIndex, int port, int numberOfTests) {
        Worker worker = mock(Worker.class);
        WorkerConnector workerConnector = WorkerConnector.createInstance(parentAddressIndex, addressIndex, port, MEMBER, null,
                worker, true);
        when(worker.getWorkerConnector()).thenReturn(workerConnector);

        TestContainer testContainer = mock(TestContainer.class, RETURNS_DEEP_STUBS);
        when(testContainer.getTestContext().getTestId()).thenReturn("ProtocolUtilTest");

        for (int testIndex = 1; testIndex <= numberOfTests; testIndex++) {
            TestOperationProcessor processor = new TestOperationProcessor(EXCEPTION_LOGGER, worker, MEMBER, testContainer,
                    workerConnector.getAddress().getChild(testIndex));
            workerConnector.addTest(testIndex, processor);
        }

        workerConnector.start();
        return workerConnector;
    }

    private static AgentConnector startAgent(int addressIndex, int port, String workerHost, int workerStartPort,
                                             int numberOfWorkers) {
        Agent agent = mock(Agent.class);
        when(agent.getAddressIndex()).thenReturn(addressIndex);

        WorkerProcessManager workerProcessManager = new WorkerProcessManager();

        AgentConnector agentConnector = AgentConnector.createInstance(agent, workerProcessManager, port, 0);
        when(agent.getAgentConnector()).thenReturn(agentConnector);

        for (int workerIndex = 1; workerIndex <= numberOfWorkers; workerIndex++) {
            agentConnector.addWorker(workerIndex, workerHost, workerStartPort + workerIndex);
        }

        agentConnector.start();
        return agentConnector;
    }

    static CoordinatorConnector startCoordinator(String agentHost, int agentStartPort, int numberOfAgents) {
        TestPhaseListeners testPhaseListeners = new TestPhaseListeners();
        PerformanceStateContainer performanceStateContainer = new PerformanceStateContainer();
        File outputDirectory = TestUtils.createTmpDirectory();
        HdrHistogramContainer hdrHistogramContainer = new HdrHistogramContainer(outputDirectory, performanceStateContainer);
        FailureContainer failureContainer = new FailureContainer(outputDirectory, null, new HashSet<FailureType>());
        CoordinatorConnector coordinatorConnector = new CoordinatorConnector(failureContainer, testPhaseListeners,
                performanceStateContainer, hdrHistogramContainer);
        for (int i = 1; i <= numberOfAgents; i++) {
            coordinatorConnector.addAgent(i, agentHost, agentStartPort + i);
        }

        return coordinatorConnector;
    }

    static SimulatorAddress getRandomDestination(int maxAddressIndex) {
        int addressLevelValue = MIN_ADDRESS_LEVEL_VALUE + RANDOM.nextInt(AddressLevel.values().length - MIN_ADDRESS_LEVEL_VALUE);
        AddressLevel addressLevel = AddressLevel.fromInt(addressLevelValue);

        int agentIndex = RANDOM.nextInt(maxAddressIndex + 1);
        int workerIndex = RANDOM.nextInt(maxAddressIndex + 1);
        int testIndex = RANDOM.nextInt(maxAddressIndex + 1);

        switch (addressLevel) {
            case COORDINATOR:
                return COORDINATOR;
            case AGENT:
                return new SimulatorAddress(addressLevel, agentIndex, 0, 0);
            case WORKER:
                return new SimulatorAddress(addressLevel, agentIndex, workerIndex, 0);
            case TEST:
                return new SimulatorAddress(addressLevel, agentIndex, workerIndex, testIndex);
            default:
                throw new IllegalArgumentException("Unsupported addressLevel: " + addressLevel);
        }
    }

    static Response sendFromCoordinator(SimulatorAddress destination) {
        return coordinatorConnector.write(destination, DEFAULT_OPERATION);
    }

    static Response sendFromCoordinator(SimulatorAddress destination, SimulatorOperation operation) {
        return coordinatorConnector.write(destination, operation);
    }

    static CoordinatorConnector getCoordinatorConnector() {
        return coordinatorConnector;
    }

    static AgentConnector getAgentConnector(int index) {
        return agentConnectors.get(index);
    }

    static WorkerConnector getWorkerConnector(int index) {
        return workerConnectors.get(index);
    }

    static void assertSingleTarget(Response response, SimulatorAddress destination, ResponseType responseType) {
        assertAllTargets(response, SimulatorAddress.COORDINATOR, destination, responseType, 1);
    }

    static void assertSingleTarget(Response response, SimulatorAddress source, SimulatorAddress destination,
                                   ResponseType responseType) {
        assertAllTargets(response, source, destination, responseType, 1);
    }

    static void assertAllTargets(Response response, SimulatorAddress destination, ResponseType responseType, int responseCount) {
        assertAllTargets(response, SimulatorAddress.COORDINATOR, destination, responseType, responseCount);
    }

    static void assertAllTargets(Response response, SimulatorAddress source, SimulatorAddress destination,
                                 ResponseType responseType, int responseCount) {
        assertEquals(source, response.getDestination());
        assertEquals(responseCount, response.entrySet().size());
        for (Map.Entry<SimulatorAddress, ResponseType> entry : response.entrySet()) {
            assertEquals(responseType, entry.getValue());
            assertEquals(destination.getAddressLevel(), entry.getKey().getAddressLevel());
        }
    }

    static void assertEmptyFutureMaps() {
        LOGGER.info("Asserting that all future maps are empty...");

        for (ClientConnector clientConnector : coordinatorConnector.getClientConnectors()) {
            ConcurrentMap<String, ResponseFuture> futureMap = clientConnector.getFutureMap();
            SimulatorAddress remoteAddress = clientConnector.getRemoteAddress();
            int futureMapSize = futureMap.size();
            if (futureMapSize > 0) {
                LOGGER.error("Future entries: " + futureMap.toString());
                fail(format("FutureMap of ClientConnector %s is not empty", remoteAddress));
            }
        }
        assertEmptyFutureMaps(agentConnectors, "AgentConnector");
        assertEmptyFutureMaps(workerConnectors, "WorkerConnector");

        LOGGER.info("Done!");
    }

    private static <C extends ServerConnector> void assertEmptyFutureMaps(List<C> connectorList, String connectorName) {
        for (C connector : connectorList) {
            ConcurrentMap<String, ResponseFuture> futureMap = connector.getFutureMap();
            int futureMapSize = futureMap.size();
            if (futureMapSize > 0) {
                LOGGER.error("Future entries: " + futureMap.toString());
                fail(format("FutureMap of %s %s is not empty", connectorName, connector.getAddress()));
            }
        }
    }
}
