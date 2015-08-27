package com.hazelcast.simulator.protocol.connector;

import com.hazelcast.simulator.protocol.configuration.WorkerServerConfiguration;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.processors.OperationProcessor;
import com.hazelcast.simulator.protocol.processors.WorkerOperationProcessor;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;

/**
 * Connector which listens for incoming Simulator Agent connections and manages Simulator Test instances.
 */
public class WorkerConnector extends AbstractServerConnector {

    private final WorkerServerConfiguration workerServerConfiguration;

    /**
     * Creates an {@link WorkerConnector}.
     *
     * @param addressIndex       the index of this Simulator Worker
     * @param parentAddressIndex the index of the parent Simulator Agent
     * @param port               the port for incoming connections
     */
    public WorkerConnector(int addressIndex, int parentAddressIndex, int port) {
        super(addressIndex, parentAddressIndex, port);
        this.workerServerConfiguration = (WorkerServerConfiguration) getConfiguration();
    }

    protected WorkerServerConfiguration createConfiguration(int addressIndex, int parentAddressIndex, int port) {
        OperationProcessor processor = new WorkerOperationProcessor(null, null);
        ConcurrentMap<String, ResponseFuture> futureMap = new ConcurrentHashMap<String, ResponseFuture>();
        SimulatorAddress localAddress = new SimulatorAddress(WORKER, parentAddressIndex, addressIndex, 0);

        return new WorkerServerConfiguration(processor, futureMap, localAddress, port);
    }

    /**
     * Adds a Simulator Test.
     *
     * @param testIndex the index of the Simulator Test
     * @param processor the {@link OperationProcessor} which processes incoming
     *                  {@link com.hazelcast.simulator.protocol.operation.SimulatorOperation} for this test
     */
    public void addTest(int testIndex, OperationProcessor processor) {
        workerServerConfiguration.addTest(testIndex, processor);
    }

    /**
     * Removes a Simulator Test.
     *
     * @param testIndex the index of the remote Simulator Test
     */
    public void removeTest(int testIndex) {
        workerServerConfiguration.removeTest(testIndex);
    }
}
