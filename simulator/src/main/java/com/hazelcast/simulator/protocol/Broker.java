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

import com.hazelcast.simulator.utils.SimulatorUtils;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.security.AuthenticationUser;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.apache.log4j.Logger;

import java.io.Closeable;

import static com.hazelcast.simulator.common.SimulatorProperties.DEFAULT_AGENT_PORT;
import static java.util.Collections.singletonList;

/**
 * A wrapper around the ActiveMQ {@link BrokerService}. Each agent will have an embedded broker instance
 * and all workers and agents listen for requests from the Coordinator.
 *
 * The initial idea of having a Broker on the Coordinator was not suitable due to routers etc. It will make
 * it difficult for the agent/workers to connect back to the coordinator machine.
 *
 * todo:
 *
 * - TestCaseRunner will wait without timeout
 *
 * - StartWorkersTask blocking indefinitely
 *
 * - closer look at InActivityMonitor: http://activemq.apache.org/activemq-inactivitymonitor.html
 *
 * - RunTestSuiteTaskTest
 *
 * - Worker has not sent a message for 60 seconds: check this logic; since worker doesn't communicate with agent
 *
 * - Server.send inefficient because producer/session not recycled and potentially many requests
 * needed:
 * http://activemq.apache.org/how-do-i-use-jms-efficiently.html
 *
 * -----------------------------------------------------------
 * nice to have
 *
 * - client only needs a single thread; not two.
 *
 * - activemq resource tuning
 *
 * - workers should be started with session id included. No need for init session
 *
 * - active mq security?
 *
 * - http://activemq.apache.org/how-do-i-embed-a-broker-inside-a-connection.html vm:// for agent (embedded broker)
 */
public class Broker implements Closeable {

    private static final Logger LOGGER = Logger.getLogger(Broker.class);
    private static final int USAGE_LIMIT = 1024;

    private BrokerService broker;
    private String brokerURL;
    private String username;
    private String password;

    public Broker() {
        setBrokerAddress(SimulatorUtils.localIp(), DEFAULT_AGENT_PORT);
    }

    public Broker setCredentials(String userName, String password) {
        this.username = userName;
        this.password = password;
        return this;
    }

    public String getBrokerURL() {
        return brokerURL;
    }

    public Broker setBrokerURL(String brokerURL) {
        this.brokerURL = brokerURL;
        return this;
    }

    public Broker setBrokerAddress(String ip, int port) {
        return setBrokerURL("tcp://" + ip + ":" + port);
    }

    public Broker start() {
        LOGGER.info("Starting broker using brokerURL: [" + brokerURL + "]");

        try {
            broker = new BrokerService();
            broker.setPersistent(false);
            broker.deleteAllMessages();
            broker.setDeleteAllMessagesOnStartup(true);
            broker.setUseJmx(false);
            broker.getSystemUsage().getTempUsage().setLimit(USAGE_LIMIT);
            broker.getSystemUsage().getStoreUsage().setLimit(USAGE_LIMIT);
            broker.addConnector(brokerURL);

            if (username != null) {
                AuthenticationUser user = new AuthenticationUser(username, password, "producers,consumer");
                SimpleAuthenticationPlugin authenticationPlugin = new SimpleAuthenticationPlugin();
                authenticationPlugin.setAnonymousAccessAllowed(false);
                authenticationPlugin.setUsers(singletonList(user));
                broker.setPlugins(new BrokerPlugin[]{authenticationPlugin});
            }

            broker.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        LOGGER.info("Successfully started broker");
        return this;
    }

    @Override
    public void close() {
        LOGGER.info("Stopping broker");
        try {
            broker.stop();
            LOGGER.info("Broker stopped");
        } catch (Exception e) {
            LOGGER.warn("Failed to stop the broker", e);
        }
    }
}
