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

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;

public class ConnectionFactory {
    private static final int MAX_THREAD_POOL_SIZE = 4;
    private static final int DEFAULT_MAX_RECONNECT_ATTEMPTS = 30;

    private String username;
    private String password;
    private int maxReconnectAttempts = DEFAULT_MAX_RECONNECT_ATTEMPTS;

    public ConnectionFactory setCredentials(String userName, String password) {
        this.username = userName;
        this.password = password;
        return this;
    }

    public void setMaxReconnectAttempts(int maxReconnectAttempts) {
        this.maxReconnectAttempts = maxReconnectAttempts;
    }

    public Connection newConnection(String brokerURL, ExceptionListener exceptionListener) throws JMSException {
        String finalBrokerURL = toUrl(brokerURL);

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(finalBrokerURL);

        // to speed things up; don't wait till it is on the socket buffer.
        // http://activemq.apache.org/async-sends.html
        connectionFactory.setUseAsyncSend(true);

        // authentication stuff.
        connectionFactory.setUserName(username);
        connectionFactory.setPassword(password);
        // we want to consume the least amount of resources possible. And there will be very low volume traffic.

        connectionFactory.setMaxThreadPoolSize(MAX_THREAD_POOL_SIZE);
        //
//        connectionFactory.setRejectedTaskHandler(new  RejectedExecutionHandler() {
//            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
//                try {
//                    executor.getQueue().put(r);
//                } catch (InterruptedException var4) {
//                    throw new RejectedExecutionException(var4);
//                }
//            }
//        });

        Connection connection = connectionFactory.createConnection();
        connection.setExceptionListener(exceptionListener);
        connection.start();
        return connection;
    }

    private String toUrl(String brokerURL) {
        // here we configure the 'failover'
        // http://activemq.apache.org/failover-transport-reference.html
        // In this case we'll allow for 30 attempts with a maximum of 1 second between the attempts.
        // so if we can't send in 30 seconds; give up.
        return "failover:(" + brokerURL + ")?initialReconnectDelay=100"
                + "&maxReconnectAttempts=" + maxReconnectAttempts
                + "&maxReconnectDelay=1000";
    }
}
