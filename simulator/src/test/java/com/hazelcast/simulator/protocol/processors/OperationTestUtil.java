package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.simulator.protocol.StubPromise;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.exception.ProcessException;
import com.hazelcast.simulator.protocol.operation.OperationType;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;


public class OperationTestUtil {

    public static ResponseType process(OperationProcessor processor, SimulatorOperation operation,
                                       SimulatorAddress address) throws Exception {
        StubPromise promise = new StubPromise();
        try {
            processor.process(operation, address, promise);
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
                                                SimulatorAddress address) throws Exception {
        StubPromise promise = new StubPromise();
        try {
            processor.processOperation(operationType, operation, address, promise);
        } catch (ProcessException e) {
            promise.answer(e.getResponseType());
        } catch (Exception e) {
            promise.answer(ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION);
        }
        return promise.join();
    }
}
