package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.simulator.protocol.StubPromise;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import com.hazelcast.simulator.protocol.exception.ProcessException;
import com.hazelcast.simulator.protocol.operation.OperationType;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;


public class OperationTestUtil {

    public static ResponseType process(OperationProcessor processor, SimulatorOperation operation,
                                       SimulatorAddress source) throws Exception {
        SimulatorMessage msg = new SimulatorMessage(null, source, 0, OperationType.getOperationType(operation), "");
        StubPromise promise = new StubPromise();
        try {
            processor.process(msg, operation, promise);
        } catch (ProcessException e) {
            promise.answer(e.getResponseType());
        } catch (Exception e) {
            promise.answer(ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION);
        }
        return promise.join();
    }

    public static ResponseType processOperation(AbstractOperationProcessor processor,
                                                OperationType operationType,
                                                SimulatorOperation operation,
                                                SimulatorAddress dest) throws Exception {
        SimulatorMessage msg = new SimulatorMessage(dest, null, 0, operationType, "");
        StubPromise promise = new StubPromise();
        try {
            processor.processOperation(msg, operation, promise);
        } catch (ProcessException e) {
            promise.answer(e.getResponseType());
        } catch (Exception e) {
            promise.answer(ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION);
        }
        return promise.join();
    }
}
