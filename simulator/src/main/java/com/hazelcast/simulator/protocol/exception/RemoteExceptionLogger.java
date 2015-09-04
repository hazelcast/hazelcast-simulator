package com.hazelcast.simulator.protocol.exception;

import com.hazelcast.simulator.protocol.connector.ServerConnector;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.ExceptionOperation;

/**
 * Sends exceptions to a {@link ServerConnector}, which will send it to the connected parent Simulator component.
 */
public class RemoteExceptionLogger extends AbstractExceptionLogger {

    private ServerConnector serverConnector;

    public RemoteExceptionLogger(SimulatorAddress localAddress, ExceptionType exceptionType) {
        super(localAddress, exceptionType);
    }

    public void setServerConnector(ServerConnector serverConnector) {
        this.serverConnector = serverConnector;
    }

    @Override
    protected void handleOperation(long exceptionId, ExceptionOperation operation) {
        serverConnector.submit(SimulatorAddress.COORDINATOR, operation);
    }
}
