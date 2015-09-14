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

import com.hazelcast.simulator.agent.Agent;
import com.hazelcast.simulator.agent.CommandFuture;
import com.hazelcast.simulator.agent.FailureAlreadyThrownRuntimeException;
import com.hazelcast.simulator.common.messaging.Message;
import com.hazelcast.simulator.common.messaging.MessageAddress;
import com.hazelcast.simulator.test.Failure;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.worker.TerminateWorkerException;
import com.hazelcast.simulator.worker.WorkerType;
import com.hazelcast.simulator.worker.commands.Command;
import com.hazelcast.simulator.worker.commands.CommandRequest;
import com.hazelcast.simulator.worker.commands.CommandResponse;
import com.hazelcast.simulator.worker.commands.MessageCommand;
import com.hazelcast.util.EmptyStatement;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.utils.CommonUtils.throwableToString;
import static com.hazelcast.simulator.utils.ExecutorFactory.createFixedThreadPool;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;

@SuppressWarnings("checkstyle:classdataabstractioncoupling")
public class WorkerJvmManager {

    public static final int PORT = 9001;
    public static final String SERVICE_POLL_WORK = "poll";
    public static final String COMMAND_PUSH_RESPONSE = "push";
    public static final File WORKERS_HOME = new File(getSimulatorHome(), "workers");

    private static final int WAIT_FOR_PROCESS_TERMINATION_TIMEOUT_MILLIS = 10000;
    private static final int EXECUTE_ON_WORKERS_TIMEOUT_SECONDS = 30;

    private static final Logger LOGGER = Logger.getLogger(WorkerJvmManager.class);

    private final ConcurrentMap<String, WorkerJvm> workerJVMs = new ConcurrentHashMap<String, WorkerJvm>();
    private final Agent agent;

    private final ConcurrentMap<Long, CommandFuture<Object>> futureMap = new ConcurrentHashMap<Long, CommandFuture<Object>>();
    private final AtomicLong requestIdGenerator = new AtomicLong(0);

    private final Executor executor = createFixedThreadPool(20, WorkerJvmManager.class);
    private final AcceptorThread acceptorThread = new AcceptorThread();
    private final Random random = new Random();

    private ServerSocket serverSocket;

    private volatile WorkerJvmSettings lastUsedWorkerJvmSettings;

    public WorkerJvmManager(Agent agent) {
        this.agent = agent;

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                terminateWorkers();
            }
        });
    }

    public void start() {
        try {
            serverSocket = new ServerSocket(PORT, 0, InetAddress.getByName(null));

            LOGGER.info("Started Worker JVM Socket on: " + serverSocket.getInetAddress().getHostAddress() + ":" + PORT);

            acceptorThread.start();
        } catch (Exception e) {
            throw new CommandLineExitException("Failed to start WorkerJvmManager", e);
        }
    }

    public void stop() {
        acceptorThread.isRunning = false;
        acceptorThread.interrupt();

        try {
            serverSocket.close();
        } catch (IOException e) {
            LOGGER.info("Exception when closing serverSocket", e);
        }
    }

    public Collection<WorkerJvm> getWorkerJVMs() {
        return workerJVMs.values();
    }

    public Object executeOnSingleWorker(Command command) throws Exception {
        List<WorkerJvm> workers = new ArrayList<WorkerJvm>(workerJVMs.values());
        if (workers.isEmpty()) {
            throw new NoWorkerAvailableException("No worker JVMs found");
        }
        workers = Collections.singletonList(workers.get(0));
        List results = executeOnWorkers(command, workers);
        if (results.isEmpty()) {
            LOGGER.info("No results found");
            return null;
        }
        return results.get(0);
    }

    public void sendMessage(Message message) throws TimeoutException, InterruptedException {
        String workerAddress = message.getMessageAddress().getWorkerAddress();
        if (MessageAddress.BROADCAST.equals(workerAddress)) {
            sendMessageToAllWorkers(message);
        } else if (MessageAddress.WORKER_WITH_OLDEST_MEMBER.equals(workerAddress)) {
            // send to all workers as they have to evaluate who is the oldest worker
            sendMessageToAllWorkers(message);
        } else if (MessageAddress.RANDOM.equals(workerAddress)) {
            sendMessageToRandomWorker(message);
        } else if (MessageAddress.ALL_WORKERS_WITH_MEMBER.equals(workerAddress)) {
            sendMessageToAllWorkersWithClusterMember(message);
        } else if (MessageAddress.RANDOM_WORKER_WITH_MEMBER.equals(workerAddress)) {
            sendMessageToRandomWorkerWithClusterMember(message);
        } else {
            throw new UnsupportedOperationException("Unsupported addressing mode for worker '" + workerAddress + "'. "
                    + "Full address: '" + message.getMessageAddress() + "'.");
        }
    }

    private void sendMessageToRandomWorkerWithClusterMember(Message message) throws TimeoutException, InterruptedException {
        WorkerJvm worker = getRandomWorkerWithMemberOrNull();
        if (worker == null) {
            LOGGER.warn("No worker is known to this agent. Is it a race-condition?");
        } else {
            List<WorkerJvm> workerList = Collections.singletonList(worker);
            preprocessMessage(message, workerList);
            Command command = new MessageCommand(message);
            executeOnWorkers(command, workerList);
        }
    }

    private void sendMessageToAllWorkersWithClusterMember(Message message) throws TimeoutException, InterruptedException {
        List<WorkerJvm> workers = getAllWorkersWithMembers();
        preprocessMessage(message, workers);
        executeOnWorkers(new MessageCommand(message), workers);
    }

    private void sendMessageToRandomWorker(Message message) throws TimeoutException, InterruptedException {
        WorkerJvm randomWorker = getRandomWorkerOrNull();
        if (randomWorker == null) {
            LOGGER.warn("No worker is known to this agent. Is it a race-condition?");
        } else {
            List<WorkerJvm> workerJvmList = Collections.singletonList(randomWorker);
            preprocessMessage(message, workerJvmList);
            Command command = new MessageCommand(message);
            executeOnWorkers(command, workerJvmList);
        }
    }

    private void preprocessMessage(Message message, Collection<WorkerJvm> workerJvmList) {
        for (WorkerJvm workerJvm : new ArrayList<WorkerJvm>(workerJvmList)) {
            if (message.removeFromAgentList()) {
                // remove worker
                while (workerJVMs.values().remove(workerJvm)) {
                    EmptyStatement.ignore(null);
                }
            } else if (message.disableMemberFailureDetection()) {
                workerJvm.stopDetectFailure();
            }
        }
    }

    private void sendMessageToAllWorkers(Message message) throws TimeoutException, InterruptedException {
        Command command = new MessageCommand(message);
        Collection<WorkerJvm> workerJvmList = workerJVMs.values();
        preprocessMessage(message, workerJvmList);
        executeOnWorkers(command, workerJvmList);
    }

    public List executeOnAllWorkers(Command command) throws TimeoutException, InterruptedException {
        return executeOnWorkers(command, workerJVMs.values());
    }

    private List executeOnWorkers(Command command, Collection<WorkerJvm> workers) throws TimeoutException, InterruptedException {
        Map<WorkerJvm, CommandFuture> futures = new HashMap<WorkerJvm, CommandFuture>();

        for (WorkerJvm workerJvm : workers) {
            if (workerJvm.isOomeDetected()) {
                continue;
            }

            CommandFuture<Object> future = new CommandFuture<Object>(command);
            CommandRequest request = new CommandRequest();
            request.id = requestIdGenerator.incrementAndGet();
            request.task = command;
            if (command.awaitReply()) {
                futureMap.put(request.id, future);
                futures.put(workerJvm, future);
            }
            workerJvm.addCommandRequest(request);
        }

        List<Object> results = new LinkedList<Object>();
        for (Map.Entry<WorkerJvm, CommandFuture> entry : futures.entrySet()) {
            WorkerJvm workerJvm = entry.getKey();
            CommandFuture future = entry.getValue();
            try {
                Object result = future.get(EXECUTE_ON_WORKERS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                results.add(result);
            } catch (TimeoutException e) {
                if (!future.getCommand().ignoreTimeout()) {
                    registerWorkerFailure(workerJvm, e);
                }
                throw e;
            } catch (ExecutionException e) {
                registerWorkerFailure(workerJvm, e);
                throw new FailureAlreadyThrownRuntimeException(e);
            }
        }
        return results;
    }

    private void registerWorkerFailure(WorkerJvm workerJvm, Exception e) {
        Failure failure = new Failure();
        failure.type = Failure.Type.WORKER_EXCEPTION;
        failure.message = e.getMessage();
        failure.agentAddress = agent.getPublicAddress();
        failure.workerAddress = workerJvm.getMemberAddress();
        failure.workerId = workerJvm.getId();
        failure.testSuite = agent.getTestSuite();
        failure.cause = throwableToString(e);
        agent.getWorkerJvmFailureMonitor().publish(failure);
    }

    public void spawn(WorkerJvmSettings settings) throws Exception {
        this.lastUsedWorkerJvmSettings = settings;
        WorkerJvmLauncher launcher = new WorkerJvmLauncher(agent, workerJVMs, settings);
        launcher.launch();
    }

    public void terminateWorkers() {
        LOGGER.info("Terminating workers");

        for (WorkerJvm jvm : new LinkedList<WorkerJvm>(workerJVMs.values())) {
            terminateWorker(jvm);
        }

        LOGGER.info("Finished terminating workers");
    }

    public void terminateRandomWorker() {
        WorkerJvm randomWorker = getRandomWorkerOrNull();
        if (randomWorker == null) {
            LOGGER.warn("Attempt to terminating a random worker detected, but no worker is running.");
            return;
        }
        terminateWorker(randomWorker);
    }

    public void terminateWorker(final WorkerJvm jvm) {
        workerJVMs.remove(jvm.getId());

        Thread t = new Thread() {
            public void run() {
                try {
                    // this sends SIGTERM on *nix
                    jvm.getProcess().destroy();
                    jvm.getProcess().waitFor();
                } catch (Throwable e) {
                    LOGGER.fatal("Failed to destroy worker process: " + jvm);
                }
            }
        };

        t.start();
        try {
            t.join(WAIT_FOR_PROCESS_TERMINATION_TIMEOUT_MILLIS);
            if (t.isAlive()) {
                LOGGER.warn("WorkerJVM is still busy terminating: " + jvm);
            }
        } catch (Exception e) {
            LOGGER.fatal("Exception in terminateWorker()", e);
        }
    }

    private WorkerJvm getRandomWorkerWithMemberOrNull() {
        List<WorkerJvm> jvmCollection = getAllWorkersWithMembers();
        if (jvmCollection.isEmpty()) {
            return null;
        }
        WorkerJvm[] workers = jvmCollection.toArray(new WorkerJvm[jvmCollection.size()]);
        return workers[random.nextInt(workers.length)];
    }

    private List<WorkerJvm> getAllWorkersWithMembers() {
        return getWorkersWithWorkerType(workerJVMs.values(), WorkerType.MEMBER);
    }

    private List<WorkerJvm> getWorkersWithWorkerType(Iterable<WorkerJvm> source, WorkerType type) {
        List<WorkerJvm> result = new ArrayList<WorkerJvm>();
        for (WorkerJvm workerJvm : source) {
            if (workerJvm.getType() == type) {
                result.add(workerJvm);
            }
        }
        return result;
    }

    private WorkerJvm getRandomWorkerOrNull() {
        Collection<WorkerJvm> jvmCollection = workerJVMs.values();
        if (jvmCollection.isEmpty()) {
            return null;
        }
        WorkerJvm[] workers = jvmCollection.toArray(new WorkerJvm[jvmCollection.size()]);
        return workers[random.nextInt(workers.length)];
    }

    public void newMember() throws Exception {
        LOGGER.info("Adding a newMember");

        WorkerJvmSettings lastUsedWorkerJvmSettings = this.lastUsedWorkerJvmSettings;
        if (lastUsedWorkerJvmSettings == null) {
            LOGGER.warn("No lastUsedWorkerJvmSettings available");
            return;
        }

        WorkerJvmSettings settings = new WorkerJvmSettings(lastUsedWorkerJvmSettings);
        settings.memberWorkerCount = 1;
        settings.clientWorkerCount = 0;

        WorkerJvmLauncher launcher = new WorkerJvmLauncher(agent, workerJVMs, settings);
        launcher.launch();
    }

    private final class ClientSocketTask implements Runnable {

        private final Socket clientSocket;

        private ClientSocketTask(Socket clientSocket) {
            this.clientSocket = clientSocket;
        }

        @Override
        public void run() {
            try {
                ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
                out.flush();

                ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());
                String service = (String) in.readObject();
                String workerId = (String) in.readObject();
                WorkerJvm workerJvm = workerJVMs.get(workerId);

                Object result = null;
                try {
                    if (workerJvm == null) {
                        LOGGER.warn("No worker JVM found for id: " + workerId);
                        result = new TerminateWorkerException();
                    } else {
                        workerJvm.updateLastSeen();
                        if (SERVICE_POLL_WORK.equals(service)) {
                            List<CommandRequest> commands = new LinkedList<CommandRequest>();
                            workerJvm.drainCommandRequests(commands);
                            result = commands;
                        } else if (COMMAND_PUSH_RESPONSE.equals(service)) {
                            CommandResponse response = (CommandResponse) in.readObject();
                            //LOGGER.info("Received response: " + response.commandId);
                            CommandFuture<Object> future = futureMap.remove(response.commandId);
                            if (future != null) {
                                future.set(response.result);
                            } else {
                                LOGGER.fatal("No future found for commandId: " + response.commandId);
                            }
                        } else {
                            throw new RuntimeException("Unknown service:" + service);
                        }
                    }
                } catch (IOException e) {
                    throw e;
                } catch (Exception e) {
                    LOGGER.fatal("Failed to process serviceId:" + service, e);
                    result = e;
                }

                out.writeObject(result);
                out.flush();
                clientSocket.close();
            } catch (ClassNotFoundException e) {
                LOGGER.fatal("ClassNotFoundException in WorkerJvmManager", e);
            } catch (IOException e) {
                LOGGER.fatal("IOException in WorkerJvmManager", e);
            }
        }
    }

    private class AcceptorThread extends Thread {

        private volatile boolean isRunning = true;

        public AcceptorThread() {
            super("AcceptorThread");
        }

        public void run() {
            while (isRunning) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Accepted worker request from: " + clientSocket.getRemoteSocketAddress());
                    }
                    executor.execute(new ClientSocketTask(clientSocket));
                } catch (Throwable e) {
                    LOGGER.fatal("Exception in AcceptorThread.run()", e);
                }
            }
        }
    }
}
