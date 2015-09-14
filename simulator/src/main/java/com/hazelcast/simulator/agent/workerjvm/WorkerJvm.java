/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.agent.workerjvm;

import com.hazelcast.simulator.worker.WorkerType;
import com.hazelcast.simulator.worker.commands.CommandRequest;

import java.io.File;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class WorkerJvm {

    private final BlockingQueue<CommandRequest> commandQueue = new LinkedBlockingQueue<CommandRequest>();

    private final String id;
    private final int index;

    private final File workerHome;
    private final WorkerType type;

    private volatile long lastSeen = System.currentTimeMillis();
    private volatile boolean detectFailure = true;

    private volatile boolean oomeDetected;

    private volatile Process process;
    private volatile String memberAddress;

    public WorkerJvm(String id, int index, File workerHome, WorkerType type) {
        this.id = id;
        this.index = index;
        this.workerHome = workerHome;
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public int getIndex() {
        return index;
    }

    public File getWorkerHome() {
        return workerHome;
    }

    public WorkerType getType() {
        return type;
    }

    public long getLastSeen() {
        return lastSeen;
    }

    public void updateLastSeen() {
        this.lastSeen = System.currentTimeMillis();
    }

    public boolean shouldDetectFailure() {
        return detectFailure;
    }

    public void stopDetectFailure() {
        this.detectFailure = false;
    }

    public boolean isOomeDetected() {
        return oomeDetected;
    }

    public void setOomeDetected() {
        this.oomeDetected = true;
    }

    public Process getProcess() {
        return process;
    }

    public void setProcess(Process process) {
        this.process = process;
    }

    public String getMemberAddress() {
        return memberAddress;
    }

    public void setMemberAddress(String memberAddress) {
        this.memberAddress = memberAddress;
    }

    public void addCommandRequest(CommandRequest request) {
        commandQueue.add(request);
    }

    public void drainCommandRequests(List<CommandRequest> commands) {
        commandQueue.drainTo(commands);
    }

    @Override
    public String toString() {
        return "WorkerJvm{"
                + "id='" + id + '\''
                + ", memberAddress='" + memberAddress + '\''
                + ", workerHome=" + workerHome
                + '}';
    }
}
