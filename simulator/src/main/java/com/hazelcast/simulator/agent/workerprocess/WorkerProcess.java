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
package com.hazelcast.simulator.agent.workerprocess;

import com.hazelcast.simulator.protocol.core.SimulatorAddress;

import java.io.File;

/**
 * Represents a worker process. So the process that does the actual work.
 */
public class WorkerProcess {

    private final SimulatorAddress address;
    private final String id;
    private final File workerHome;
    private volatile long lastSeen = System.currentTimeMillis();
    private volatile boolean oomeDetected;
    private volatile boolean isFinished;
    private volatile Process process;
    private volatile String pid;

    public WorkerProcess(SimulatorAddress address, String id, File workerHome) {
        this.address = address;
        this.id = id;
        this.workerHome = workerHome;
    }

    public SimulatorAddress getAddress() {
        return address;
    }

    public String getId() {
        return id;
    }

    public File getWorkerHome() {
        return workerHome;
    }

    public long getLastSeen() {
        return lastSeen;
    }

    public void updateLastSeen() {
        this.lastSeen = System.currentTimeMillis();
    }

    public void setLastSeen(long timeStamp) {
        this.lastSeen = timeStamp;
    }

    public boolean isOomeDetected() {
        return oomeDetected;
    }

    public void setOomeDetected() {
        this.oomeDetected = true;
    }

    public boolean isFinished() {
        return isFinished;
    }

    public void setFinished() {
        isFinished = true;
    }

    public Process getProcess() {
        return process;
    }

    public void setProcess(Process process) {
        this.process = process;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }
}
