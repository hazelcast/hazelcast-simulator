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
package com.hazelcast.simulator.agent;

import com.hazelcast.simulator.agent.workerprocess.WorkerProcessManager;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.utils.EmptyStatement;
import org.apache.log4j.Logger;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;

/**
 * Sniffs the traffic of workers that publish on the coordinator topic and updates the 'lastSeen' accordingly
 * on the {@link WorkerProcessManager}.
 *
 * Since each agent has its own broker, all traffic on the coordinator topic will be of workers owned by this
 * agent.
 */
public class WorkerSniffer {

    private static final Logger LOGGER = Logger.getLogger(WorkerSniffer.class);

    private Connection connection;
    private final WorkerProcessManager processManager;
    private volatile boolean stop;
    private final SnifferThread snifferThread = new SnifferThread();
    private Session session;
    private Topic coordinatorTopic;
    private MessageConsumer consumer;

    public WorkerSniffer(WorkerProcessManager processManager) {
        this.processManager = processManager;
    }

    public WorkerSniffer setConnection(Connection connection) {
        this.connection = connection;
        return this;
    }

    public void start() {
//        try {
//            this.session = connection.createSession(false, AUTO_ACKNOWLEDGE);
//            this.coordinatorTopic = session.createTopic("coordinator");
//            this.consumer = session.createConsumer(coordinatorTopic);
//            snifferThread.start();
//            LOGGER.info("Coordinator topic sniffer started");
//        } catch (JMSException e) {
//            throw new RuntimeException(e);
//        }
    }

    public void stop() {
//        stop = true;
//        snifferThread.interrupt();
//
//        try {
//            snifferThread.join(MINUTES.toMillis(1));
//        } catch (InterruptedException e) {
//            Thread.currentThread().interrupt();
//        }
//
//        if (snifferThread.isAlive()) {
//            LOGGER.info("Failed to stop the server in the given timeout");
//        } else {
//            LOGGER.info("Successfully stopped Server");
//        }
    }

    private class SnifferThread extends Thread {
        @Override
        public void run() {
            try {
                while (!stop) {
                    try {
                        run0();
                    } catch (Exception e) {
                        if (!stop) {
                            LOGGER.error(e);
                        }
                    }
                }
            } finally {
                LOGGER.info("Sniffer thread stopped");
                closeSilently();
            }
        }

        private void run0() throws Exception {
            Message m = consumer.receive();
            SimulatorAddress address = SimulatorAddress.fromString(m.getStringProperty("source"));
            processManager.updateLastSeenTimestamp(address);
        }

        private void closeSilently() {
            try {
                consumer.close();
                session.close();
            } catch (Exception ignore) {
                EmptyStatement.ignore(ignore);
            }
        }
    }
}
