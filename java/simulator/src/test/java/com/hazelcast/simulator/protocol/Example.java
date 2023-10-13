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
package com.hazelcast.simulator.protocol;

import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.message.LogMessage;
import com.hazelcast.simulator.protocol.message.SimulatorMessage;

import java.net.Inet4Address;
import java.util.concurrent.Future;

/**
 * Created by alarmnummer on 3/4/17.
 */
public class Example {

    public static void main(String[] args) throws Exception {
        String username = "peter";
        String password = "password";

        Broker broker = new Broker()
                .setCredentials(username, password)
                .start();

        CoordinatorClient coordinatorClient = new CoordinatorClient()
                .connectToAgentBroker(SimulatorAddress.fromString("A1"), Inet4Address.getLocalHost().getHostAddress())
                .start();

        Server agentServer = new Server("agents")
                .setProcessor(new MessageHandler() {
                    @Override
                    public void process(SimulatorMessage msg, SimulatorAddress source, Promise promise) throws Exception {
                    }
                })
                .setSelfAddress(SimulatorAddress.fromString("A1"))
                .setBrokerURL(broker.getBrokerURL())
                .start();

        Server workerServer = new Server("workers")
                .setProcessor(new MessageHandler() {
                    @Override
                    public void process(SimulatorMessage msg, SimulatorAddress source, Promise promise) throws Exception {
                        System.out.println("worker:" + msg);
                    }
                })
                .setSelfAddress(SimulatorAddress.fromString("A1_W1"))
                .setBrokerURL(broker.getBrokerURL())
                .start();


        SimulatorAddress address = SimulatorAddress.fromString("A1_W1");

        Future f = coordinatorClient.submit(address, new LogMessage("foo"));
        System.out.println(f.get());


//        client.send(address, new AuthOperation());

        Thread.sleep(5000);

        agentServer.close();
        workerServer.close();
        coordinatorClient.close();

        broker.close();
    }
}
