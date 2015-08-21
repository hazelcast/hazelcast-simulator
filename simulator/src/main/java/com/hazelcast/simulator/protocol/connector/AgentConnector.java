package com.hazelcast.simulator.protocol.connector;

import com.hazelcast.simulator.protocol.configuration.AgentServerConfiguration;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import com.hazelcast.simulator.protocol.processors.AgentOperationProcessor;

import static com.hazelcast.simulator.protocol.core.AddressLevel.AGENT;
import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;

/**
 * Connector which listens for incoming Simulator Coordinator connections and connects to remote Simulator Worker instances.
 */
public class AgentConnector {

    private final AgentServerConfiguration configuration;
    private final ServerConnector server;
    private final SimulatorAddress localAddress;

    /**
     * Creates an {@link AgentConnector}.
     *
     * @param addressIndex the index of this Simulator Agent
     * @param port         the port for incoming connections
     */
    public AgentConnector(int addressIndex, int port) {
        SimulatorAddress localAddress = new SimulatorAddress(AGENT, addressIndex, 0, 0);
        AgentOperationProcessor processor = new AgentOperationProcessor();

        this.configuration = new AgentServerConfiguration(localAddress, addressIndex, port, processor);
        this.server = new ServerConnector(configuration);
        this.localAddress = new SimulatorAddress(AGENT, configuration.getAddressIndex(), 0, 0);
    }

    /**
     * Starts to listen on the incoming port.
     */
    public void start() {
        server.start();
    }

    /**
     * Stops to listen on the incoming port.
     */
    public void shutdown() {
        server.shutdown();
    }

    /**
     * Adds a Simulator Worker and connects to it.
     *
     * @param workerIndex the index of the Simulator Worker
     * @param remoteHost  the host of the Simulator Worker
     * @param remotePort  the port of the Simulator Worker
     */
    public void addWorker(int workerIndex, String remoteHost, int remotePort) {
        // TODO: spawn Simulator Worker instance

        ClientConnector client = new ClientConnector(localAddress, WORKER, workerIndex, remoteHost, remotePort);
        client.start();

        configuration.addWorker(workerIndex, client);
    }

    /**
     * Removes a Simulator Worker.
     *
     * @param workerIndex the index of the remote Simulator Worker
     */
    public void removeWorker(int workerIndex) {
        configuration.removeWorker(workerIndex);
    }

    public void write(SimulatorMessage message) throws Exception {
        server.write(message);
    }
}
