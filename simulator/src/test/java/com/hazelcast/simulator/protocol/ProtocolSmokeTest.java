package com.hazelcast.simulator.protocol;

import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

import static com.hazelcast.simulator.TestEnvironmentUtils.resetLogLevel;
import static com.hazelcast.simulator.TestEnvironmentUtils.setLogLevel;
import static com.hazelcast.simulator.protocol.ProtocolUtil.assertEmptyFutureMaps;
import static com.hazelcast.simulator.protocol.ProtocolUtil.getRandomDestination;
import static com.hazelcast.simulator.protocol.ProtocolUtil.sendFromCoordinator;
import static com.hazelcast.simulator.protocol.ProtocolUtil.startSimulatorComponents;
import static com.hazelcast.simulator.protocol.ProtocolUtil.stopSimulatorComponents;
import static com.hazelcast.simulator.protocol.core.AddressLevel.AGENT;
import static com.hazelcast.simulator.protocol.core.AddressLevel.TEST;
import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ProtocolSmokeTest {

    private static final int NUMBER_OF_MESSAGES = 5000;

    private static final int NUMBER_OF_AGENTS = 2;
    private static final int NUMBER_OF_WORKERS = 3;
    private static final int NUMBER_OF_TESTS = 4;

    private static final int MAX_ADDRESS_INDEX = 5;

    private static final int DEFAULT_TEST_TIMEOUT_MILLIS = NUMBER_OF_MESSAGES * 10;

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
    }

    @After
    public void commonAsserts() {
        assertEmptyFutureMaps();
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void smokeTest() {
        for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            SimulatorAddress destination = getRandomDestination(MAX_ADDRESS_INDEX);

            LOGGER.info(format("C sending message to %s", destination));
            Response response = sendFromCoordinator(destination);
            long messageId = response.getMessageId();

            // log response
            boolean responseSuccess = true;
            for (Map.Entry<SimulatorAddress, ResponseType> entry : response.entrySet()) {
                SimulatorAddress responseSource = entry.getKey();
                ResponseType responseType = entry.getValue();
                switch (responseType) {
                    case SUCCESS:
                        LOGGER.info(format("[%d] %s %s", messageId, responseSource, responseType));
                        break;
                    case FAILURE_AGENT_NOT_FOUND:
                    case FAILURE_WORKER_NOT_FOUND:
                    case FAILURE_TEST_NOT_FOUND:
                        logNotFoundError(responseType, messageId, responseSource, destination);
                        responseSuccess = false;
                        break;
                    default:
                        LOGGER.error(format("[%d] %s %s (unexpected)", messageId, responseSource, responseType));
                        fail(format("Unexpected responseType %s for %s", response, destination));
                }
            }

            // assert response
            assertEquals(SimulatorAddress.COORDINATOR, response.getDestination());
            assertEquals(expectResponseSuccess(destination), responseSuccess);
            if (responseSuccess) {
                assertEquals(getNumberOfTargets(destination), response.entrySet().size());
            } else {
                assertTrue(response.entrySet().size() > 0);
            }
        }
    }

    private static void logNotFoundError(ResponseType responseType, long messageId, SimulatorAddress src, SimulatorAddress dst) {
        switch (responseType) {
            case FAILURE_AGENT_NOT_FOUND:
                LOGGER.error(format("[%d] %s has no %s A%s", messageId, src, AGENT, dst.getAgentIndex()));
                break;
            case FAILURE_WORKER_NOT_FOUND:
                LOGGER.error(format("[%d] %s has no %s W%s", messageId, src, WORKER, dst.getWorkerIndex()));
                break;
            default:
                LOGGER.error(format("[%d] %s has no %s T%s", messageId, src, TEST, dst.getTestIndex()));
        }
    }

    private static boolean expectResponseSuccess(SimulatorAddress destination) {
        if (destination.getAddressLevel() == AddressLevel.COORDINATOR) {
            return true;
        }
        if (destination.getAgentIndex() > NUMBER_OF_AGENTS) {
            return false;
        }
        AddressLevel addressLevel = destination.getAddressLevel();
        if (AGENT.isParentAddressLevel(addressLevel) && destination.getWorkerIndex() > NUMBER_OF_WORKERS) {
            return false;
        }
        if (WORKER.isParentAddressLevel(addressLevel) && destination.getTestIndex() > NUMBER_OF_TESTS) {
            return false;
        }
        return true;
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
