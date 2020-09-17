/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.coordinator.operations.RcDownloadOperation;
import com.hazelcast.simulator.coordinator.operations.RcInstallOperation;
import com.hazelcast.simulator.coordinator.operations.RcPrintLayoutOperation;
import com.hazelcast.simulator.coordinator.operations.RcStopCoordinatorOperation;
import com.hazelcast.simulator.coordinator.operations.RcTestRunOperation;
import com.hazelcast.simulator.coordinator.operations.RcTestStatusOperation;
import com.hazelcast.simulator.coordinator.operations.RcTestStopOperation;
import com.hazelcast.simulator.coordinator.operations.RcWorkerKillOperation;
import com.hazelcast.simulator.coordinator.operations.RcWorkerScriptOperation;
import com.hazelcast.simulator.coordinator.operations.RcWorkerStartOperation;
import com.hazelcast.simulator.protocol.exception.ProcessException;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;

/**
 * Serverside implementation of the CoordinatorRemote.
 */
public class CoordinatorRemoteImpl implements CoordinatorRemote {

    private final Coordinator coordinator;

    public CoordinatorRemoteImpl(Coordinator coordinator) {
        this.coordinator = coordinator;
    }

    @Override
    public String execute(SimulatorOperation op) throws Exception {
        if (op instanceof RcDownloadOperation) {
            coordinator.download();
        } else if (op instanceof RcInstallOperation) {
            RcInstallOperation installOp = (RcInstallOperation) op;
            coordinator.installDriver(installOp.getVersionSpec());
        } else if (op instanceof RcPrintLayoutOperation) {
            return coordinator.printLayout();
        } else if (op instanceof RcStopCoordinatorOperation) {
            coordinator.stop();
        } else if (op instanceof RcTestRunOperation) {
            return coordinator.testRun((RcTestRunOperation) op);
        } else if (op instanceof RcTestStatusOperation) {
            return coordinator.testStatus((RcTestStatusOperation) op);
        } else if (op instanceof RcTestStopOperation) {
            return coordinator.testStop((RcTestStopOperation) op);
        } else if (op instanceof RcWorkerKillOperation) {
            return coordinator.workerKill((RcWorkerKillOperation) op);
        } else if (op instanceof RcWorkerScriptOperation) {
            return coordinator.workerScript((RcWorkerScriptOperation) op);
        } else if (op instanceof RcWorkerStartOperation) {
            return coordinator.workerStart((RcWorkerStartOperation) op);
        } else {
            throw new ProcessException("Unknown operation:" + op);
        }
        return null;
    }
}
