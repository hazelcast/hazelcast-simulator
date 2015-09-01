package com.hazelcast.simulator.protocol.connector;

import com.hazelcast.simulator.protocol.configuration.ClientConfiguration;
import com.hazelcast.simulator.protocol.configuration.CoordinatorClientConfiguration;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import com.hazelcast.simulator.protocol.exception.LocalExceptionLogger;
import com.hazelcast.simulator.protocol.processors.CoordinatorOperationProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.simulator.protocol.core.ResponseType.FAILURE_AGENT_NOT_FOUND;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.utils.CommonUtils.joinThreads;

/**
 * Connector which connects to remote Simulator Agent instances.
 */
public class CoordinatorConnector {

    private final LocalExceptionLogger exceptionLogger = new LocalExceptionLogger();
    private final CoordinatorOperationProcessor processor = new CoordinatorOperationProcessor(exceptionLogger);
    private final ConcurrentMap<Integer, ClientConnector> agents = new ConcurrentHashMap<Integer, ClientConnector>();

    /**
     * Disconnects from all Simulator Agent instances.
     */
    public void shutdown() {
        List<Thread> shutdownThreads = new ArrayList<Thread>();
        for (final ClientConnector agent : agents.values()) {
            Thread thread = new Thread() {
                @Override
                public void run() {
                    agent.shutdown();
                }
            };
            thread.start();
            shutdownThreads.add(thread);
        }
        joinThreads(shutdownThreads);
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
     * @param message the {@link SimulatorMessage} to send
     * @return a {@link Response} with the response of all addressed Simulator components.
     * @throws Exception if the send method was interrupted or an exception occurred
     */
    public Response send(SimulatorMessage message) throws Exception {
        int agentAddressIndex = message.getDestination().getAgentIndex();
        Response response = new Response(message);
        if (agentAddressIndex == 0) {
            List<ResponseFuture> futureList = new ArrayList<ResponseFuture>();
            for (ClientConnector agent : agents.values()) {
                futureList.add(agent.writeAsync(message));
            }
            for (ResponseFuture future : futureList) {
                response.addResponse(future.get());
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
