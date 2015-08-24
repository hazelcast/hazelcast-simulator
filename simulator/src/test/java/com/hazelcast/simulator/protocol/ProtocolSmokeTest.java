package com.hazelcast.simulator.protocol;

import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

import static com.hazelcast.simulator.protocol.ProtocolUtil.buildRandomMessage;
import static com.hazelcast.simulator.protocol.ProtocolUtil.resetLogLevel;
import static com.hazelcast.simulator.protocol.ProtocolUtil.resetMessageId;
import static com.hazelcast.simulator.protocol.ProtocolUtil.sendFromCoordinator;
import static com.hazelcast.simulator.protocol.ProtocolUtil.setLogLevel;
import static com.hazelcast.simulator.protocol.ProtocolUtil.startSimulatorComponents;
import static com.hazelcast.simulator.protocol.ProtocolUtil.stopSimulatorComponents;
import static com.hazelcast.simulator.protocol.core.AddressLevel.AGENT;
import static com.hazelcast.simulator.protocol.core.AddressLevel.TEST;
import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ProtocolSmokeTest {

    private static final int NUMBER_OF_MESSAGES = 5000;
    private static final int DEFAULT_TEST_TIMEOUT = NUMBER_OF_MESSAGES * 5;

    private static final int NUMBER_OF_AGENTS = 2;
    private static final int NUMBER_OF_WORKERS = 2;
    private static final int NUMBER_OF_TESTS = 2;

    private static final Logger LOGGER = Logger.getLogger(ProtocolSmokeTest.class);

    @BeforeClass
    public static void setUp() {
        setLogLevel(Level.INFO);

        startSimulatorComponents(NUMBER_OF_AGENTS, NUMBER_OF_WORKERS, NUMBER_OF_TESTS);
    }

    @AfterClass
    public static void tearDown() {
        stopSimulatorComponents();

        resetLogLevel();
        resetMessageId();
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void smokeTest() throws Exception {
        for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            SimulatorMessage message = buildRandomMessage();
            long messageId = message.getMessageId();

            LOGGER.info(format("[%d] C sending message to %s", messageId, message.getDestination()));
            Response response = sendFromCoordinator(message);

            // log response
            boolean responseSuccess = true;
            for (Map.Entry<SimulatorAddress, ResponseType> entry : response.entrySet()) {
                SimulatorAddress responseSource = entry.getKey();
                ResponseType responseType = entry.getValue();
                switch (responseType) {
                    case FAILURE_AGENT_NOT_FOUND:
                        logNotFoundError(messageId, responseSource, message.getDestination(), AGENT);
                        responseSuccess = false;
                        break;
                    case FAILURE_WORKER_NOT_FOUND:
                        logNotFoundError(messageId, responseSource, message.getDestination(), WORKER);
                        responseSuccess = false;
                        break;
                    case FAILURE_TEST_NOT_FOUND:
                        logNotFoundError(messageId, responseSource, message.getDestination(), TEST);
                        responseSuccess = false;
                        break;
                    case SUCCESS:
                        LOGGER.info(format("[%d] %s %s", messageId, responseSource, responseType));
                        break;
                    default:
                        LOGGER.error(format("[%d] %s %s", messageId, responseSource, responseType));
                        responseSuccess = false;
                }
            }

            // assert response
            assertEquals(message.getMessageId(), response.getMessageId());
            assertEquals(message.getSource(), response.getDestination());
            if (responseSuccess) {
                assertEquals(getNumberOfTargets(message.getDestination()), response.entrySet().size());
            } else {
                assertTrue(response.entrySet().size() > 0);
            }
        }
    }

    private static void logNotFoundError(long messageId, SimulatorAddress src, SimulatorAddress dst, AddressLevel addressLevel) {
        String childNotFound;
        switch (addressLevel) {
            case AGENT:
                childNotFound = "A" + dst.getAgentIndex();
                break;
            case WORKER:
                childNotFound = "W" + dst.getWorkerIndex();
                break;
            default:
                childNotFound = "T" + dst.getTestIndex();
        }
        LOGGER.error(format("[%d] %s has no %s %s", messageId, src, addressLevel, childNotFound));
    }

    private static int getNumberOfTargets(SimulatorAddress destination) {
        int numberOfTargets = 1;
        switch (destination.getAddressLevel()) {
            case TEST:
                if (destination.getTestIndex() == 0) {
                    numberOfTargets *= NUMBER_OF_TESTS;
                }
            case WORKER:
                if (destination.getWorkerIndex() == 0) {
                    numberOfTargets *= NUMBER_OF_WORKERS;
                }
            case AGENT:
                if (destination.getAgentIndex() == 0) {
                    numberOfTargets *= NUMBER_OF_AGENTS;
                }
        }
        return numberOfTargets;
    }
}
