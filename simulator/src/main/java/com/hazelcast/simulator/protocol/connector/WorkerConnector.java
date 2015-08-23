package com.hazelcast.simulator.protocol.connector;

import com.hazelcast.simulator.protocol.configuration.WorkerServerConfiguration;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import com.hazelcast.simulator.protocol.processors.OperationProcessor;
import com.hazelcast.simulator.protocol.processors.WorkerOperationProcessor;

import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;

/**
 * Connector which listens for incoming Simulator Agent connections and manages Simulator Test instances.
 */
public class WorkerConnector {

    private final WorkerServerConfiguration configuration;
    private final ServerConnector server;

    /**
     * Creates an {@link WorkerConnector}.
     *
     * @param addressIndex       the index of this Simulator Worker
     * @param parentAddressIndex the index of the parent Simulator Agent
     * @param port               the port for incoming connections
     */
    public WorkerConnector(int addressIndex, int parentAddressIndex, int port) {
        OperationProcessor processor = new WorkerOperationProcessor();
        SimulatorAddress localAddress = new SimulatorAddress(WORKER, parentAddressIndex, addressIndex, 0);

        this.configuration = new WorkerServerConfiguration(processor, localAddress, port);
        this.server = new ServerConnector(configuration);
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
     * Adds a Simulator Test.
     *
     * @param testIndex the index of the Simulator Test
     * @param processor the {@link OperationProcessor} which processes incoming
     *                  {@link com.hazelcast.simulator.protocol.operation.SimulatorOperation} for this test
     */
    public void addTest(int testIndex, OperationProcessor processor) {
        // TODO: create a TestContainer instance

        configuration.addTest(testIndex, processor);
    }

    /**
     * Removes a Simulator Test.
     *
     * @param testIndex the index of the remote Simulator Test
     */
    public void removeTest(int testIndex) {
        configuration.removeTest(testIndex);
    }

    public SimulatorAddress getAddress() {
        return configuration.getLocalAddress();
    }

    public Response write(SimulatorMessage message) throws Exception {
        return server.write(message);
    }
}
