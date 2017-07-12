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

import com.hazelcast.simulator.common.FailureType;
import com.hazelcast.simulator.coordinator.FailureCollector;
import com.hazelcast.simulator.coordinator.operations.FailureOperation;
import com.hazelcast.simulator.coordinator.registry.AgentData;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.OperationCodec;
import com.hazelcast.simulator.protocol.operation.OperationType;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.utils.SimulatorUtils;
import org.apache.log4j.Logger;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.simulator.common.SimulatorProperties.DEFAULT_AGENT_PORT;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.coordinatorAddress;
import static com.hazelcast.simulator.protocol.operation.OperationType.getOperationType;
import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.UuidUtil.newUnsecureUuidString;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static javax.jms.DeliveryMode.NON_PERSISTENT;

/**
 * Responsible for connecting to the agents and sending operations or invoking operations.
 */
public class CoordinatorClient implements Closeable {

    private static final Logger LOGGER = Logger.getLogger(CoordinatorClient.class);

    private final BlockingQueue<SendTask> taskQueue = new LinkedBlockingQueue<SendTask>();
    // the key is the agent-index
    private final ConcurrentMap<Integer, RemoteBroker> remoteBrokers
            = new ConcurrentHashMap<Integer, RemoteBroker>();
    private final ConcurrentMap<String, FutureImpl> futures = new ConcurrentHashMap<String, FutureImpl>();
    private final SendThread sendThread;
    private final ConnectionFactory connectionFactory = new ConnectionFactory();
    private ResponseHandlerThread responseHandlerThread;
    private OperationProcessor processor;
    private int remoteBrokerPort = DEFAULT_AGENT_PORT;
    private FailureCollector failureCollector;
    private volatile boolean stop;

    public CoordinatorClient() {
        this.responseHandlerThread = new ResponseHandlerThread();
        this.sendThread = new SendThread();
    }

    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public CoordinatorClient setFailureCollector(FailureCollector failureCollector) {
        this.failureCollector = failureCollector;
        return this;
    }

    public CoordinatorClient setAgentBrokerPort(int port) {
        this.remoteBrokerPort = port;
        return this;
    }

    public CoordinatorClient connectToAgentBroker(SimulatorAddress agentAddress, String agentIp) throws JMSException {
        if (agentIp.equals("localhost")) {
            agentIp = SimulatorUtils.localIp();
        }
        remoteBrokers.put(agentAddress.getAgentIndex(), new RemoteBroker(agentIp, agentAddress));
        return this;
    }

    public CoordinatorClient setProcessor(OperationProcessor processor) {
        this.processor = processor;
        return this;
    }

    public CoordinatorClient start() {
        responseHandlerThread.start();
        sendThread.start();
        return this;
    }

    public void send(SimulatorAddress target, SimulatorOperation op) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("sending " + op + " to " + target);
        }
        taskQueue.add(new SendTask(target, getRemoteBroker(target), op, null));
    }

    public Future<String> submit(SimulatorAddress target, SimulatorOperation op) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("sending " + op + " to " + target);
        }

        RemoteBroker remoteBroker = getRemoteBroker(target);

        FutureImpl future = new FutureImpl(remoteBroker);
        futures.put(future.messageId, future);

        taskQueue.add(new SendTask(target, remoteBroker, op, future.messageId));

        return future;
    }

    public List<String> invokeAll(List<AgentData> agents, SimulatorOperation op, long timeoutMillis)
            throws TimeoutException, InterruptedException, ExecutionException {
        Map<AgentData, Future<String>> futures = new HashMap<AgentData, Future<String>>();
        for (AgentData agent : agents) {
            futures.put(agent, submit(agent.getAddress(), op));
        }

        List<String> responses = new ArrayList<String>(agents.size());
        long deadLine = currentTimeMillis() + timeoutMillis;

        for (Map.Entry<AgentData, Future<String>> entry : futures.entrySet()) {
            long remainingTimeout = deadLine - currentTimeMillis();

            if (remainingTimeout <= 0) {
                throw new TimeoutException();
            }

            Future<String> f = entry.getValue();
            responses.add(f.get(remainingTimeout, MILLISECONDS));
        }

        return responses;
    }

    private RemoteBroker getRemoteBroker(SimulatorAddress target) {
        RemoteBroker broker = remoteBrokers.get(target.getAgentIndex());

        if (broker == null) {
            throw new IllegalArgumentException("Could not find a broker for [" + target + "]");
        }

        return broker;
    }

    @Override
    public void close() {
        stop = true;
        sendThread.interrupt();
        responseHandlerThread.interrupt();

        closeQuietly(remoteBrokers.values());
        remoteBrokers.clear();
    }

    static class FutureImpl implements Future<String> {
        private final RemoteBroker agentBroker;
        private final String messageId = newUnsecureUuidString();
        private volatile Object result;

        public FutureImpl(RemoteBroker agentBroker) {
            this.agentBroker = agentBroker;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return result != null;
        }

        @Override
        public String get() throws InterruptedException, ExecutionException {
            synchronized (this) {
                while (result == null) {
                    wait();
                }
            }

            if (result instanceof Throwable) {
                throw new ExecutionException((Throwable) result);
            } else {
                return (String) result;
            }
        }

        @Override
        public String get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            long deadlineMillis = currentTimeMillis() + unit.toMillis(timeout);

            synchronized (this) {
                while (result == null) {
                    long remainingTime = deadlineMillis - currentTimeMillis();
                    if (remainingTime <= 0) {
                        throw new TimeoutException();
                    }
                    wait(deadlineMillis);
                }
            }

            if (result instanceof Throwable) {
                throw new ExecutionException((Throwable) result);
            } else {
                return (String) result;
            }
        }

        public void complete(Object payload) {
            synchronized (this) {
                if (this.result != null) {
                    return;
                }

                this.result = payload;
                notifyAll();
            }
        }
    }

    class SendTask {

        private final RemoteBroker remoteBroker;
        private final SimulatorOperation op;
        private final String requestId;
        private final SimulatorAddress target;

        SendTask(SimulatorAddress target, RemoteBroker remoteBroker, SimulatorOperation op, String requestId) {
            this.target = target;
            this.remoteBroker = remoteBroker;
            this.op = op;
            this.requestId = requestId;
        }

        private void run() throws JMSException {
            Message message = remoteBroker.session.createMessage();

            if (requestId != null) {
                message.setJMSReplyTo(remoteBroker.replyQueue);
                message.setJMSCorrelationID(requestId);
            }

            message.setStringProperty("source", coordinatorAddress().toString());
            message.setStringProperty("target", target.toString());
            message.setStringProperty("payload", OperationCodec.toJson(op));
            message.setIntProperty("operationType", getOperationType(op).toInt());

            switch (target.getAddressLevel()) {
                case AGENT:
                    remoteBroker.agentProducer.send(message);
                    break;
                case WORKER:
                    remoteBroker.workerProducer.send(message);
                    break;
                default:
                    throw new RuntimeException("unhandled target:" + target);
            }
        }
    }

    class SendThread extends Thread {
        @Override
        public void run() {
            while (!stop) {
                try {
                    taskQueue.take().run();
                } catch (Throwable e) {
                    if (!stop) {
                        LOGGER.error(e.getMessage(), e);
                    }
                }
            }
        }
    }

    final class RemoteBroker implements Closeable, ExceptionListener {
        private volatile boolean closed;
        private final Session session;
        private final Connection connection;
        private final MessageProducer agentProducer;
        private final MessageProducer workerProducer;
        // the queue the coordinator receives replies on
        private final Queue replyQueue;
        private final MessageConsumer replyQueueConsumer;
        private final MessageConsumer coordinatorConsumer;
        private final SimulatorAddress agentAddress;
        private boolean connected;

        private RemoteBroker(String ip, SimulatorAddress agentAddress) throws JMSException {
            this.agentAddress = agentAddress;

            connection = connectionFactory.newConnection("tcp://" + ip + ":" + remoteBrokerPort, this);
            connected = true;
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            agentProducer = session.createProducer(session.createTopic("agents"));
            agentProducer.setDeliveryMode(NON_PERSISTENT);

            workerProducer = session.createProducer(session.createTopic("workers"));
            workerProducer.setDeliveryMode(NON_PERSISTENT);

            coordinatorConsumer = session.createConsumer(session.createTopic("coordinator"));

            replyQueue = session.createQueue(newUnsecureUuidString());
            replyQueueConsumer = session.createConsumer(replyQueue);

            LOGGER.info(format("Successfully connected to agent [%s]", agentAddress));
        }

        @Override
        public void onException(JMSException e) {
            close();

            if (connected) {
                LOGGER.fatal("Lost connection to agent [" + agentAddress + "], cause [" + e.getMessage() + "]");
            } else {
                LOGGER.fatal("Failed to connect to agent [" + agentAddress + "], cause [" + e.getMessage() + "]");
            }
            LOGGER.debug(e.getMessage(), e);

            remoteBrokers.remove(agentAddress.getAgentIndex());

            FailureOperation failureOperation = new FailureOperation(
                    "Lost connection to " + agentAddress,
                    FailureType.MESSAGING_EXCEPTION,
                    null,
                    agentAddress.toString(),
                    e);

            if (failureCollector != null) {
                failureCollector.notify(failureOperation);
            }
        }

        @Override
        public void close() {
            closed = true;
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (JMSException e) {
                LOGGER.trace("Failed to close connection " + connection, e);
            }
        }
    }

    private class ResponseHandlerThread extends Thread {

        private static final int DELAY_MILLIS = 100;

        @Override
        public void run() {
            try {
                while (!stop) {
                    boolean hasResponse;
                    do {
                        checkDeadFutures();

                        hasResponse = false;
                        for (RemoteBroker remoteBroker : remoteBrokers.values()) {
                            if (processResponses(remoteBroker)) {
                                hasResponse = true;
                            }
                            if (processMessages(remoteBroker)) {
                                hasResponse = true;
                            }
                        }

                    } while (hasResponse);

                    Thread.sleep(DELAY_MILLIS);
                }
            } catch (Throwable e) {
                if (!stop) {
                    LOGGER.fatal(e.getMessage(), e);
                }
            }
        }

        private void checkDeadFutures() {
            for (Map.Entry<String, FutureImpl> entry : futures.entrySet()) {
                FutureImpl f = entry.getValue();
                if (f.agentBroker.closed) {
                    futures.remove(entry.getKey());
                    f.complete(new JMSException("Connection to broker " + f.agentBroker.agentAddress + "is closed"));
                }
            }
        }

        private boolean processMessages(RemoteBroker remoteBroker) {
            try {
                Message message = remoteBroker.coordinatorConsumer.receiveNoWait();
                if (message == null) {
                    return false;
                }

                OperationType operationType = OperationType.fromInt(message.getIntProperty("operationType"));
                String operationData = message.getStringProperty("payload");

                SimulatorOperation op = OperationCodec.fromJson(operationData, operationType.getClassType());
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Received " + op);
                }

                SimulatorAddress source = SimulatorAddress.fromString(message.getStringProperty("source"));

                processor.process(op, source, EmptyPromise.INSTANCE);
                return true;
            } catch (Exception e) {
                if (!stop) {
                    //todo: feed into failure collector
                    LOGGER.fatal(e.getMessage(), e);
                }
                return false;
            }
        }

        private boolean processResponses(RemoteBroker remoteBroker) {
            try {
                Message replyMessage = remoteBroker.replyQueueConsumer.receiveNoWait();
                if (replyMessage == null) {
                    return false;
                }

                String correlationId = replyMessage.getJMSCorrelationID();
                FutureImpl future = futures.remove(correlationId);
                if (future == null) {
                    LOGGER.debug("No future for " + correlationId + "\n" + replyMessage);
                } else {
                    boolean error = replyMessage.getBooleanProperty("error");
                    if (error) {
                        String message = replyMessage.getStringProperty("message");
                        future.complete(new Exception(message));
                    } else {
                        future.complete(replyMessage.getStringProperty("payload"));
                    }
                }
                return true;
            } catch (Exception e) {
                if (!stop) {
                    //todo: feed into failure collector
                    LOGGER.fatal(e);
                }
                return false;
            }
        }
    }
}
