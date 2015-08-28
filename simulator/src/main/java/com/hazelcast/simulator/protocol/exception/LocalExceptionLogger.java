package com.hazelcast.simulator.protocol.exception;

import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.ExceptionOperation;
import com.hazelcast.simulator.protocol.processors.CoordinatorOperationProcessor;
import org.apache.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class LocalExceptionLogger extends AbstractExceptionLogger {

    private static final Logger LOGGER = Logger.getLogger(CoordinatorOperationProcessor.class);

    private final BlockingQueue<ExceptionOperation> exceptionList = new LinkedBlockingQueue<ExceptionOperation>();

    public LocalExceptionLogger() {
        this(SimulatorAddress.COORDINATOR, ExceptionType.COORDINATOR_EXCEPTION);
    }

    public LocalExceptionLogger(SimulatorAddress localAddress, ExceptionType exceptionType) {
        super(localAddress, exceptionType);
    }

    public int getExceptionCount() {
        return exceptionList.size();
    }

    @Override
    protected void handleOperation(long exceptionId, ExceptionOperation operation) {
        logOperation(operation);
    }

    public void logOperation(ExceptionOperation operation) {
        exceptionList.add(operation);
        LOGGER.warn(operation.getConsoleLog(exceptionList.size()));

        // TODO: get TestCase and add this to the failures file instead of LOGGER
        LOGGER.warn(operation.getFileLog(null));
    }
}
