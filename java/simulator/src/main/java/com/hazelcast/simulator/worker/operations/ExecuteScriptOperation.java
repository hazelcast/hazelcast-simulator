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
package com.hazelcast.simulator.worker.operations;

import com.google.gson.annotations.SerializedName;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;

/**
 * Execute a script, e.g. bash or javascript on a worker. This is useful to e.g. obtain some state, or cause problems like
 * OOME.
 */
public class ExecuteScriptOperation implements SimulatorOperation {

    /**
     * The script to execute.
     */
    @SerializedName("command")
    private String command;

    /**
     * If the script is a fire and forget (so no response being send) or not.
     *
     * Fire and forget is useful for e.g. causing problems on a member/client and observe the consequences
     * of it in the test. E.g. allocate fake memory till 90% of the heap gets filled and see what happens.
     */
    @SerializedName("fireAndForget")
    private boolean fireAndForget;

    public ExecuteScriptOperation(String command, boolean fireAndForget) {
        this.command = command;
        this.fireAndForget = fireAndForget;
    }

    public String getCommand() {
        return command;
    }

    public boolean isFireAndForget() {
        return fireAndForget;
    }
}
