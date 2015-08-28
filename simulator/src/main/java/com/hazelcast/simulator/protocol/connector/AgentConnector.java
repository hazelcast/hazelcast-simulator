package com.hazelcast.simulator.protocol.connector;

import com.hazelcast.simulator.protocol.configuration.AgentServerConfiguration;
import com.hazelcast.simulator.protocol.configuration.ClientConfiguration;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.exception.ExceptionType;
import com.hazelcast.simulator.protocol.exception.RemoteExceptionLogger;
import com.hazelcast.simulator.protocol.processors.AgentOperationProcessor;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.simulator.protocol.core.AddressLevel.AGENT;

/**
 * Connector which listens for incoming Simulator Coordinator connections and manages Simulator Worker instances.
 */
public final class AgentConnector extends AbstractServerConnector {

    private final AgentServerConfiguration serverConfiguration;

    private AgentConnector(AgentServerConfiguration configuration) {
        super(configuration);
        this.serverConfiguration = configuration;
    }

    /**
     * Creates an {@link AgentConnector} instance.
     *
     * @param addressIndex the index of this Simulator Agent
     * @param port         the port for incoming connections
     */
    public static AgentConnector createInstance(int addressIndex, int port) {
        SimulatorAddress localAddress = new SimulatorAddress(AGENT, addressIndex, 0, 0);

        RemoteExceptionLogger exceptionLogger = new RemoteExceptionLogger(localAddress, ExceptionType.AGENT_EXCEPTION);
        AgentOperationProcessor processor = new AgentOperationProcessor(exceptionLogger);
        ConcurrentMap<String, ResponseFuture> futureMap = new ConcurrentHashMap<String, ResponseFuture>();

        AgentServerConfiguration configuration = new AgentServerConfiguration(processor, futureMap, localAddress, port);
        AgentConnector connector = new AgentConnector(configuration);

        exceptionLogger.setServerConnector(connector);
        return connector;
    }

    /**
     * Adds a Simulator Worker and connects to it.
     *
     * @param workerIndex the index of the Simulator Worker
     * @param workerHost  the host of the Simulator Worker
     * @param workerPort  the port of the Simulator Worker
     */
    public void addWorker(int workerIndex, String workerHost, int workerPort) {
        ClientConfiguration clientConfiguration = serverConfiguration.getClientConfiguration(workerIndex, workerHost, workerPort);
        ClientConnector client = new ClientConnector(clientConfiguration);
        client.start();

        serverConfiguration.addWorker(workerIndex, client);
    }

    /**
     * Removes a Simulator Worker.
     *
     * @param workerIndex the index of the remote Simulator Worker
     */
    public void removeWorker(int workerIndex) {
        serverConfiguration.removeWorker(workerIndex);
    }
}
