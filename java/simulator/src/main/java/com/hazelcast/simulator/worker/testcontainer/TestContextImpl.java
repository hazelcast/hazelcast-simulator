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
package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.simulator.probes.LatencyProbe;
import com.hazelcast.simulator.probes.impl.NoopLatencyProbe;
import com.hazelcast.simulator.probes.impl.HdrLatencyProbe;
import com.hazelcast.simulator.protocol.Server;
import com.hazelcast.simulator.protocol.message.LogMessage;
import com.hazelcast.simulator.test.TestContext;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.lang.String.format;

public class TestContextImpl implements TestContext {

    private final String testId;
    private final String publicIpAddress;
    private final Server server;
    private final ConcurrentMap<String, LatencyProbe> latencyProbes = new ConcurrentHashMap<>();
    private volatile boolean stopped;
    private Class latencyProbeClass;

    public TestContextImpl(String testId,
                           String publicIpAddress,
                           Server server) {
        this.testId = testId;
        this.publicIpAddress = publicIpAddress;
        this.server = server;
    }

    public void setLatencyProbeClass(Class latencyProbeClass) {
        this.latencyProbeClass = latencyProbeClass;
    }

    public Map<String, LatencyProbe> getLatencyProbes() {
        return latencyProbes;
    }

    public LatencyProbe getLatencyProbe(String probeName, boolean includeInThroughput) {
        if (probeName == null) {
            throw new RuntimeException("probeName can't be null");
        }

        if (latencyProbeClass == null) {
            return NoopLatencyProbe.INSTANCE;
        }

        LatencyProbe probe = latencyProbes.get(probeName);
        if (probe == null) {
            probe = new HdrLatencyProbe(probeName, includeInThroughput);
            LatencyProbe found = latencyProbes.putIfAbsent(probeName, probe);
            if (found != null) {
                probe = found;
            }
        }
        return probe;
    }

    @Override
    public String getTestId() {
        return testId;
    }

    @Override
    public String getPublicIpAddress() {
        return publicIpAddress;
    }

    @Override
    public boolean isStopped() {
        return stopped;
    }

    @Override
    public void stop() {
        stopped = true;
    }

    @Override
    public void echoCoordinator(String msg, Object... args) {
        String message = format(msg, args);
        server.sendCoordinator(new LogMessage(message));
    }
}
