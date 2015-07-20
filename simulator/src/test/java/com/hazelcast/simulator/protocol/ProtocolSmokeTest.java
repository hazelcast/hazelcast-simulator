package com.hazelcast.simulator.protocol;

import com.hazelcast.simulator.protocol.connector.AgentConnector;
import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.connector.WorkerConnector;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.simulator.protocol.ProtocolUtil.buildRandomMessage;
import static com.hazelcast.simulator.protocol.ProtocolUtil.startAgent;
import static com.hazelcast.simulator.protocol.ProtocolUtil.startCoordinator;
import static com.hazelcast.simulator.protocol.ProtocolUtil.startWorker;
import static com.hazelcast.simulator.protocol.core.AddressLevel.AGENT;
import static com.hazelcast.simulator.protocol.core.AddressLevel.TEST;
import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;
import static java.lang.String.format;

public class ProtocolSmokeTest {

    private static final int NUMBER_OF_MESSAGES = 5000;

    private static final Logger LOGGER = Logger.getLogger(ProtocolSmokeTest.class);

    private CoordinatorConnector coordinatorConnector;
    private List<AgentConnector> agentConnectors = new ArrayList<AgentConnector>();
    private List<WorkerConnector> workerConnectors = new ArrayList<WorkerConnector>();

    @Before
    public void setUp() {
        LOGGER.setLevel(Level.INFO);

        workerConnectors.add(startWorker(1, 1, 10011));
        workerConnectors.add(startWorker(2, 1, 10012));

        workerConnectors.add(startWorker(1, 2, 10021));
        workerConnectors.add(startWorker(2, 2, 10022));

        agentConnectors.add(startAgent(1, 10001, "127.0.0.1", 10010));
        agentConnectors.add(startAgent(2, 10002, "127.0.0.1", 10020));

        coordinatorConnector = startCoordinator("127.0.0.1", 10000);
    }

    @After
    public void tearDown() {
        LOGGER.info("Shutdown of Coordinator...");
        if (coordinatorConnector != null) {
            coordinatorConnector.shutdown();
        }

        LOGGER.info("Shutdown of Agents...");
        for (AgentConnector agentConnector : agentConnectors) {
            agentConnector.shutdown();
        }

        LOGGER.info("Shutdown of Workers...");
        for (WorkerConnector workerConnector : workerConnectors) {
            workerConnector.shutdown();
        }

        LOGGER.info("Shutdown complete!");
    }

    @Test(timeout = 30000)
    public void sendMessages() throws Exception {
        for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            SimulatorMessage message = buildRandomMessage();
            LOGGER.info(format("[%d] C sending message to %s", message.getMessageId(), message.getDestination()));

            Response response = coordinatorConnector.send(message);
            for (Map.Entry<SimulatorAddress, ResponseType> entry : response.entrySet()) {
                long messageId = response.getMessageId();
                SimulatorAddress responseSource = entry.getKey();
                ResponseType responseType = entry.getValue();
                switch (responseType) {
                    case FAILURE_AGENT_NOT_FOUND:
                        logNotFoundError(messageId, responseSource, message.getDestination(), AGENT);
                        break;
                    case FAILURE_WORKER_NOT_FOUND:
                        logNotFoundError(messageId, responseSource, message.getDestination(), WORKER);
                        break;
                    case FAILURE_TEST_NOT_FOUND:
                        logNotFoundError(messageId, responseSource, message.getDestination(), TEST);
                        break;
                    case SUCCESS:
                        LOGGER.info(format("[%d] %s %s", messageId, responseSource, responseType));
                        break;
                    default:
                        LOGGER.error(format("[%d] %s %s", messageId, responseSource, responseType));
                }
            }
        }
    }

    private static void logNotFoundError(long messageId, SimulatorAddress src, SimulatorAddress dst, AddressLevel addressLevel) {
        String childNotFound;
        if (addressLevel == AGENT) {
            childNotFound = "A" + dst.getAgentIndex();
        } else if (addressLevel == WORKER) {
            childNotFound = "W" + dst.getWorkerIndex();
        } else {
            childNotFound = "T" + dst.getTestIndex();
        }
        LOGGER.error(format("[%d] %s has no %s %s", messageId, src, addressLevel, childNotFound));
    }
}
