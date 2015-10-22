package com.hazelcast.simulator.protocol.connector;

import com.hazelcast.simulator.coordinator.FailureContainer;
import com.hazelcast.simulator.coordinator.PerformanceStateContainer;
import com.hazelcast.simulator.coordinator.TestHistogramContainer;
import com.hazelcast.simulator.protocol.configuration.ClientConfiguration;
import com.hazelcast.simulator.protocol.configuration.CoordinatorClientConfiguration;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import com.hazelcast.simulator.protocol.core.SimulatorProtocolException;
import com.hazelcast.simulator.protocol.exception.LocalExceptionLogger;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.processors.CoordinatorOperationProcessor;
import com.hazelcast.simulator.utils.ThreadSpawner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.protocol.core.ResponseType.FAILURE_AGENT_NOT_FOUND;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.protocol.operation.OperationCodec.toJson;
import static com.hazelcast.simulator.protocol.operation.OperationType.getOperationType;

/**
 * Connector which connects to remote Simulator Agent instances.
 */
public class CoordinatorConnector {

    private final AtomicLong messageIds = new AtomicLong();
    private final ConcurrentMap<Integer, ClientConnector> agents = new ConcurrentHashMap<Integer, ClientConnector>();
    private final LocalExceptionLogger exceptionLogger = new LocalExceptionLogger();

    private final CoordinatorOperationProcessor processor;

    public CoordinatorConnector(PerformanceStateContainer performanceStateContainer,
                                TestHistogramContainer testHistogramContainer, FailureContainer failureContainer) {
        this.processor = new CoordinatorOperationProcessor(exceptionLogger, performanceStateContainer, testHistogramContainer,
                failureContainer);
    }

    /**
     * Disconnects from all Simulator Agent instances.
     */
    public void shutdown() {
        ThreadSpawner spawner = new ThreadSpawner("shutdownClientConnectors", true);
        for (final ClientConnector agent : agents.values()) {
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    agent.shutdown();
                }
            });
        }
        spawner.awaitCompletion();

        processor.shutdown();
    }

    /**
     * Adds a Simulator Agent and connects to it.
     *
     * @param agentIndex the index of the Simulator Agent
     * @param agentHost  the host of the Simulator Agent
     * @param agentPort  the port of the Simulator Agent
     */
    public void addAgent(int agentIndex, String agentHost, int agentPort) {
        ClientConfiguration clientConfiguration = new CoordinatorClientConfiguration(processor, agentIndex, agentHost, agentPort);
        ClientConnector client = new ClientConnector(clientConfiguration);
        client.start();

        agents.put(agentIndex, client);
    }

    /**
     * Removes a Simulator Agent.
     *
     * @param agentIndex the index of the remote Simulator Agent
     */
    public void removeAgent(int agentIndex) {
        ClientConnector clientConnector = agents.remove(agentIndex);
        if (clientConnector != null) {
            clientConnector.shutdown();
        }
    }

    /**
     * Sends a {@link SimulatorMessage} to the addressed Simulator component.
     *
     * @param destination the {@link SimulatorAddress} of the destination
     * @param operation   the {@link SimulatorOperation} to send
     * @return a {@link Response} with the response of all addressed Simulator components.
     */
    public Response write(SimulatorAddress destination, SimulatorOperation operation) {
        SimulatorMessage message = new SimulatorMessage(destination, COORDINATOR, messageIds.incrementAndGet(),
                getOperationType(operation), toJson(operation));

        int agentAddressIndex = destination.getAgentIndex();
        Response response = new Response(message);
        if (agentAddressIndex == 0) {
            List<ResponseFuture> futureList = new ArrayList<ResponseFuture>();
            for (ClientConnector agent : agents.values()) {
                futureList.add(agent.writeAsync(message));
            }
            try {
                for (ResponseFuture future : futureList) {
                    response.addResponse(future.get());
                }
            } catch (InterruptedException e) {
                throw new SimulatorProtocolException("ResponseFuture.get() got interrupted!", e);
            }
        } else {
            ClientConnector agent = agents.get(agentAddressIndex);
            if (agent == null) {
                response.addResponse(COORDINATOR, FAILURE_AGENT_NOT_FOUND);
            } else {
                response.addResponse(agent.write(message));
            }
        }
        return response;
    }

    /**
     * Returns the number of collected exceptions.
     *
     * @return the number of exceptions.
     */
    public int getExceptionCount() {
        return exceptionLogger.getExceptionCount();
    }

    /**
     * Returns a list of {@link ClientConfiguration} from all connected {@link ClientConnector} instances.
     *
     * @return a list of {@link ClientConfiguration}
     */
    public List<ClientConfiguration> getConfigurationList() {
        List<ClientConfiguration> configurations = new ArrayList<ClientConfiguration>(agents.size());
        for (ClientConnector clientConnector : agents.values()) {
            configurations.add(clientConnector.getConfiguration());
        }
        return configurations;
    }
}
