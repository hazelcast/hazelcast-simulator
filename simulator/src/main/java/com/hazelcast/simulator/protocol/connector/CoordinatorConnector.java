package com.hazelcast.simulator.protocol.connector;

import com.hazelcast.simulator.protocol.configuration.ClientConfiguration;
import com.hazelcast.simulator.protocol.configuration.CoordinatorClientConfiguration;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import com.hazelcast.simulator.protocol.processors.CoordinatorOperationProcessor;
import com.hazelcast.simulator.protocol.processors.OperationProcessor;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.simulator.protocol.core.ResponseType.FAILURE_AGENT_NOT_FOUND;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;

/**
 * Connector which connects to remote Simulator Agent instances.
 */
public class CoordinatorConnector {

    private final ConcurrentMap<Integer, ClientConnector> agents = new ConcurrentHashMap<Integer, ClientConnector>();
    private final OperationProcessor processor = new CoordinatorOperationProcessor();

    /**
     * Disconnects from all Simulator Agent instances.
     */
    public void shutdown() {
        for (ClientConnector agent : agents.values()) {
            agent.shutdown();
        }
    }

    /**
     * Adds a Simulator Agent and connects to it.
     *
     * @param agentIndex the index of the Simulator Agent
     * @param remoteHost the host of the Simulator Agent
     * @param remotePort the port of the Simulator Agent
     */
    public void addAgent(int agentIndex, String remoteHost, int remotePort) {
        // TODO: spawn Simulator Agent instance

        ClientConfiguration clientConfiguration = new CoordinatorClientConfiguration(agentIndex, remoteHost, remotePort,
                processor);
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
        Response response = new Response(message.getMessageId());
        if (agentAddressIndex == 0) {
            for (ClientConnector agent : agents.values()) {
                response.addResponse(agent.write(message));
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
}
