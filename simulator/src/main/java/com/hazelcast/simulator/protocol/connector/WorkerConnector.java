package com.hazelcast.simulator.protocol.connector;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.protocol.configuration.WorkerServerConfiguration;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.exception.ExceptionLogger;
import com.hazelcast.simulator.protocol.exception.ExceptionType;
import com.hazelcast.simulator.protocol.exception.FileExceptionLogger;
import com.hazelcast.simulator.protocol.exception.RemoteExceptionLogger;
import com.hazelcast.simulator.protocol.processors.OperationProcessor;
import com.hazelcast.simulator.protocol.processors.WorkerOperationProcessor;
import com.hazelcast.simulator.worker.Worker;
import com.hazelcast.simulator.worker.WorkerType;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;

/**
 * Connector which listens for incoming Simulator Agent connections and manages Simulator Test instances.
 */
public final class WorkerConnector extends AbstractServerConnector {

    private final WorkerServerConfiguration workerServerConfiguration;

    private WorkerConnector(WorkerServerConfiguration configuration) {
        super(configuration);
        this.workerServerConfiguration = configuration;
    }

    /**
     * Creates a {@link WorkerConnector} instance.
     *
     * @param parentAddressIndex the index of the parent Simulator Agent
     * @param addressIndex       the index of this Simulator Worker
     * @param port               the port for incoming connections
     * @param type               the {@link WorkerType} of this Simulator Worker
     * @param hazelcastInstance  the {@link HazelcastInstance} for this Simulator Worker
     * @param worker             the {@link Worker} instance of this Simulator Worker
     */
    public static WorkerConnector createInstance(int parentAddressIndex, int addressIndex, int port, WorkerType type,
                                                 HazelcastInstance hazelcastInstance, Worker worker) {
        return createInstance(parentAddressIndex, addressIndex, port, type, hazelcastInstance, worker, false);
    }

    /**
     * Creates a {@link WorkerConnector} instance.
     *
     * @param parentAddressIndex the index of the parent Simulator Agent
     * @param addressIndex       the index of this Simulator Worker
     * @param port               the port for incoming connections
     * @param type               the {@link WorkerType} of this Simulator Worker
     * @param hazelcastInstance  the {@link HazelcastInstance} for this Simulator Worker
     * @param worker             the {@link Worker} instance of this Simulator Worker
     * @param useRemoteLogger    determines if the {@link RemoteExceptionLogger} or {@link FileExceptionLogger} should be used
     */
    public static WorkerConnector createInstance(int parentAddressIndex, int addressIndex, int port, WorkerType type,
                                                 HazelcastInstance hazelcastInstance, Worker worker, boolean useRemoteLogger) {
        SimulatorAddress localAddress = new SimulatorAddress(WORKER, parentAddressIndex, addressIndex, 0);

        ExceptionLogger exceptionLogger;
        if (useRemoteLogger) {
            exceptionLogger = new RemoteExceptionLogger(localAddress, ExceptionType.WORKER_EXCEPTION);
        } else {
            exceptionLogger = new FileExceptionLogger(localAddress, ExceptionType.WORKER_EXCEPTION);
        }
        WorkerOperationProcessor processor = new WorkerOperationProcessor(exceptionLogger, type, hazelcastInstance, worker);
        ConcurrentMap<String, ResponseFuture> futureMap = new ConcurrentHashMap<String, ResponseFuture>();

        WorkerServerConfiguration configuration = new WorkerServerConfiguration(processor, futureMap, localAddress, port);
        WorkerConnector connector = new WorkerConnector(configuration);

        if (useRemoteLogger) {
            ((RemoteExceptionLogger) exceptionLogger).setServerConnector(connector);
        }
        return connector;
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
