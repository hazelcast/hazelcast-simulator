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
import com.hazelcast.simulator.protocol.operation.OperationCodec;
import com.hazelcast.simulator.protocol.operation.OperationType;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import java.io.Closeable;

import static com.hazelcast.simulator.common.SimulatorProperties.DEFAULT_AGENT_PORT;
import static com.hazelcast.simulator.protocol.operation.OperationType.getOperationType;
import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.Preconditions.checkNotNull;
import static com.hazelcast.simulator.utils.SimulatorUtils.localIp;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static javax.jms.DeliveryMode.NON_PERSISTENT;


/**
 * A Server will listen to a topic for requests and will process them.
 *
 * A each agent and worker will have a Server instance to listen to requests from the Coordinator.
 *
 * If you are a client, this is the class you want to study thoroughly. It contains most of the logic needed for understanding
 * how to integrate your client.
 */
public class Server implements Closeable {
    private static final Logger LOGGER = LogManager.getLogger(Server.class);

    private final String topic;
    private final ConnectionFactory connectionFactory = new ConnectionFactory();
    private final ServerThread serverThread = new ServerThread();
    private SimulatorAddress selfAddress;
    private OperationProcessor processor;
    private MessageConsumer consumer;
    private Session session;
    private Topic destination;
    private Connection connection;
    private String brokerURL;
    private String selfAddressString;
    private ExceptionListener exceptionListener = e -> LOGGER.error("JMS Exception occurred", e);

    private volatile boolean stop;

    public Server(String topic) {
        this.topic = checkNotNull(topic, "topic can't be null");
        setBrokerURL(localIp(), DEFAULT_AGENT_PORT);
    }

    public Server setExceptionListener(ExceptionListener exceptionListener) {
        this.exceptionListener = exceptionListener;
        return this;
    }

    /**
     * Sets the SimulatorAddress of this server. For example if this server is running on A1_W3, then
     * that is the address that needs to be set. It is used for 2 purposes:
     * <ol>
     * <li>for a source addresses on messages being send (e.g. replies)</li>
     * <li>to listen to messages only meant for this server.</li>
     * </ol>
     *
     * @param selfAddress the address of this server.
     * @return this;
     */
    public Server setSelfAddress(SimulatorAddress selfAddress) {
        this.selfAddress = selfAddress;
        this.selfAddressString = selfAddress.toString();
        return this;
    }

    /**
     * Set the {@link OperationProcessor} responsible for handling operations.
     *
     * @param processor the OperationProcessor.
     * @return this;
     */
    public Server setProcessor(OperationProcessor processor) {
        this.processor = processor;
        return this;
    }

    public Server setBrokerURL(String brokerURL) {
        this.brokerURL = brokerURL;
        return this;
    }

    public Server setBrokerURL(String ip, int port) {
        return setBrokerURL("tcp://" + ip + ":" + port);
    }

    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public Connection getConnection() {
        return connection;
    }

    public Server start() {
        LOGGER.info("Starting server [" + brokerURL + "] on topic [" + topic + "]");

        try {
            this.connection = connectionFactory.newConnection(brokerURL, exceptionListener);
            this.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            this.destination = session.createTopic(topic);

            // we need to add the 'target=selfAddress' as a filter to only receive message we should
            // receive. Otherwise we'll process messages meant for others.
            String selector = "target='" + selfAddress + "'";
            LOGGER.info(format("Using messageSelector [%s]", selector));
            this.consumer = session.createConsumer(destination, selector);
            serverThread.start();

            LOGGER.info("Successfully started server for " + selfAddressString);
            return this;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        stop = true;
        serverThread.interrupt();
        closeQuietly(connection);
        LOGGER.info("Server Stopped");
    }

    public void sendCoordinator(SimulatorOperation op) {
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("sending [" + op + "]");
            }

            Destination topic = session.createTopic("coordinator");
            MessageProducer producer = session.createProducer(topic);
            producer.setTimeToLive(MINUTES.toMillis(1));
            producer.setDeliveryMode(NON_PERSISTENT);

            Message message = session.createMessage();
            message.setStringProperty("source", selfAddressString);
            message.setStringProperty("payload", OperationCodec.toJson(op));
            message.setIntProperty("operationType", getOperationType(op).toInt());

            producer.send(message);
        } catch (JMSException e) {
            LOGGER.error(e);
        }
    }

    private class PromiseImpl implements Promise {
        private Destination replyTo;
        private String correlationId;
        private SimulatorOperation op;

        @Override
        public void answer(Object o) {
            if (replyTo == null) {
                return;
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("Sending reply [%s] for [%s] to %s", o, op, replyTo));
            }

            try {
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Message message = session.createMessage();
                message.setJMSCorrelationID(correlationId);
                message.setStringProperty("source", selfAddressString);

                if (o instanceof Throwable) {
                    Throwable throwable = (Throwable) o;
                    message.setBooleanProperty("error", true);
                    message.setStringProperty("message", throwable.getMessage());
                } else {
                    message.setBooleanProperty("error", false);
                    // hack
                    message.setStringProperty("payload", "" + o);
                }

                MessageProducer producer = session.createProducer(replyTo);
                producer.send(message);
                session.close();
            } catch (JMSException e) {
                LOGGER.error(e);
            }
        }
    }

    private class ServerThread extends Thread {

        @Override
        public void run() {
            try {
                while (!stop) {
                    handle();
                }
            } catch (Throwable e) {
                if (!stop) {
                    LOGGER.error(e.getMessage(), e);
                }
            }

            LOGGER.info("ServerThread finished");
        }

        private void handle() throws Exception {
            Message message = consumer.receive();

            OperationType operationType = OperationType.fromInt(message.getIntProperty("operationType"));
            String operationData = message.getStringProperty("payload");
            SimulatorOperation op = OperationCodec.fromJson(operationData, operationType.getClassType());
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Received operation:" + op);
            }
            PromiseImpl promise = new PromiseImpl();
            promise.replyTo = message.getJMSReplyTo();
            promise.correlationId = message.getJMSCorrelationID();
            promise.op = op;

            SimulatorAddress source = SimulatorAddress.fromString(message.getStringProperty("source"));

            try {
                processor.process(op, source, promise);
            } catch (Exception e) {
                if (stop) {
                    throw e;
                } else {
                    LOGGER.warn(e.getMessage(), e);
                    promise.answer(e);
                }
            }
        }
    }
}
