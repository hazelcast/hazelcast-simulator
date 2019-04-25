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

package com.hazelcast.simulator.hz;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.utils.ExceptionReporter;

import java.io.File;
import java.io.IOException;

public class ConnectDisconnectTest extends HazelcastTest {

    public long timeoutMillis = 5000;

    private ClientConfig clientConfig;

    @Setup
    public void setup() throws IOException {
        clientConfig = new XmlClientConfigBuilder(new File("client-hazelcast.xml")).build();
        targetInstance.shutdown();
    }

    @TimeStep
    public void timestep() throws Exception {
        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
                    client.shutdown();
                } catch (Exception e) {
                    ExceptionReporter.report(testContext.getTestId(), e);
                }
            }
        };
        thread.start();
        thread.join(timeoutMillis);
        if (thread.isAlive()) {
            throw new RuntimeException("Connect/Disconnect cycle failed to complete in " + timeoutMillis + " millis");
        }
    }
}
