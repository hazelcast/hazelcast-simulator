/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.stabilizer.agent.workerjvm;

import com.hazelcast.stabilizer.agent.Agent;
import com.hazelcast.stabilizer.agent.CommandFuture;
import com.hazelcast.stabilizer.agent.FailureAlreadyThrownRuntimeException;
import com.hazelcast.stabilizer.common.messaging.Message;
import com.hazelcast.stabilizer.common.messaging.MessageAddress;
import com.hazelcast.stabilizer.test.Failure;
import com.hazelcast.stabilizer.worker.TerminateWorkerException;
import com.hazelcast.stabilizer.worker.commands.Command;
import com.hazelcast.stabilizer.worker.commands.CommandRequest;
import com.hazelcast.stabilizer.worker.commands.CommandResponse;
import com.hazelcast.stabilizer.worker.commands.MessageCommand;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.stabilizer.utils.CommonUtils.getHostAddress;
import static com.hazelcast.stabilizer.utils.CommonUtils.throwableToString;
import static com.hazelcast.stabilizer.utils.FileUtils.getStablizerHome;

public class WorkerJvmManager {

    public final static String SERVICE_POLL_WORK = "poll";
    public final static String COMMAND_PUSH_RESPONSE = "push";
    public static final int PORT = 9001;
    public final static File WORKERS_HOME = new File(getStablizerHome(), "workers");

    private final static Logger log = Logger.getLogger(WorkerJvmManager.class);
    private static final int WAIT_FOR_PROCESS_TERMINATION_TIMEOUT_MILLIS = 10000;

    private final ConcurrentMap<String, WorkerJvm> workerJvms = new ConcurrentHashMap<String, WorkerJvm>();
    private final Agent agent;

    private final ConcurrentMap<Long, CommandFuture> futureMap = new ConcurrentHashMap<Long, CommandFuture>();
    private final AtomicLong requestIdGenerator = new AtomicLong(0);

    private ServerSocket serverSocket;
    private final Executor executor = Executors.newFixedThreadPool(20);
    private Random random = new Random();

    private volatile WorkerJvmSettings lastUsedWorkerJvmSettings;

    public WorkerJvmManager(Agent agent) {
        this.agent = agent;

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                terminateWorkers();
            }
        });
    }

    public void start() throws Exception {
        serverSocket = new ServerSocket(PORT, 0, InetAddress.getByName(null));

        log.info("Started Worker JVM Socket on: " + serverSocket.getInetAddress().getHostAddress() + ":" + PORT);

        new AcceptorThread().start();
    }

    public Collection<WorkerJvm> getWorkerJvms() {
        return workerJvms.values();
    }

    public Object executeOnSingleWorker(Command command) throws Exception {
        List<WorkerJvm> workers = new ArrayList<WorkerJvm>(workerJvms.values());
        if (workers.isEmpty()) {
            throw new NoWorkerAvailableException("No worker JVM's found");
        }
        workers = Collections.singletonList(workers.get(0));
        List results = executeOnWorkers(command, workers);
        if (results.isEmpty()) {
            log.info("No results found");
            return null;
        }
        return results.get(0);
    }

    public void sendMessage(Message message) throws TimeoutException, InterruptedException {
        String workerAddress = message.getMessageAddress().getWorkerAddress();
        if (MessageAddress.BROADCAST.equals(workerAddress)) {
            sendMessageToAllWorkers(message);
        } else if (MessageAddress.WORKER_WITH_OLDEST_MEMBER.equals(workerAddress)) {
            sendMessageToAllWorkers(message); //send to all workers as they have to evaluate who is the oldest worker
        } else if (MessageAddress.RANDOM.equals(workerAddress)) {
            sendMessageToRandomWorker(message);
        } else if (MessageAddress.ALL_WORKERS_WITH_MEMBER.equals(workerAddress)) {
            sendMessageToAllWorkersWithClusterMember(message);
        } else if (MessageAddress.RANDOM_WORKER_WITH_MEMBER.equals(workerAddress)) {
            sendMessageToRandomWorkerWithClusterMember(message);
        } else {
            throw new UnsupportedOperationException("Unsupported addressing mode for worker '" + workerAddress + "'. " +
                    "Full address: '" + message.getMessageAddress() + "'.");
        }
    }

    private void sendMessageToRandomWorkerWithClusterMember(Message message) throws TimeoutException, InterruptedException {
        WorkerJvm worker = getRandomWorkerWithClusterMemberOrNull();
        if (worker == null) {
            log.warn("No worker is known to this agent. Is it a race-condition?");
        } else {
            List<WorkerJvm> workerList = Collections.singletonList(worker);
            preprocessMessage(message, workerList);
            Command command = new MessageCommand(message);
            executeOnWorkers(command, workerList);
        }
    }

    private void sendMessageToAllWorkersWithClusterMember(Message message) throws TimeoutException, InterruptedException {
        List<WorkerJvm> workers = getAllWorkersWithClusterMembers();
        preprocessMessage(message, workers);
        executeOnWorkers(new MessageCommand(message), workers);
    }

    private void sendMessageToRandomWorker(Message message) throws TimeoutException, InterruptedException {
        WorkerJvm randomWorker = getRandomWorkerOrNull();
        if (randomWorker == null) {
            log.warn("No worker is known to this agent. Is it a race-condition?");
        } else {
            List<WorkerJvm> workerJvmList = Arrays.asList(randomWorker);
            preprocessMessage(message, workerJvmList);
            Command command = new MessageCommand(message);
            executeOnWorkers(command, workerJvmList);
        }
    }

    private void preprocessMessage(Message message, Collection<WorkerJvm> workerJvmList) {
        for (WorkerJvm workerJvm : new ArrayList<WorkerJvm>(workerJvmList)) {
            if (message.removeFromAgentList()) {
                while (workerJvms.values().remove(workerJvm)); //remove worker
            } else if (message.disableMemberFailureDetection()) {
                workerJvm.detectFailure = false;
            }
        }
    }

    private void sendMessageToAllWorkers(Message message) throws TimeoutException, InterruptedException {
        Command command = new MessageCommand(message);
        Collection<WorkerJvm> workerJvmList = workerJvms.values();
        preprocessMessage(message, workerJvmList);
        executeOnWorkers(command, workerJvmList);
    }

    public List executeOnAllWorkers(Command command) throws TimeoutException, InterruptedException {
        return executeOnWorkers(command, workerJvms.values());
    }

    private List executeOnWorkers(Command command, Collection<WorkerJvm> workers) throws TimeoutException, InterruptedException {
        Map<WorkerJvm, CommandFuture> futures = new HashMap<WorkerJvm, CommandFuture>();

        for (WorkerJvm workerJvm : workers) {
            if (workerJvm.oomeDetected) {
                continue;
            }

            CommandFuture future = new CommandFuture(command);
            CommandRequest request = new CommandRequest();
            request.id = requestIdGenerator.incrementAndGet();
            request.task = command;
            if (command.awaitReply()) {
                futureMap.put(request.id, future);
                futures.put(workerJvm, future);
            }
            workerJvm.commandQueue.add(request);
        }

        List<Object> results = new LinkedList<Object>();
        for (Map.Entry<WorkerJvm, CommandFuture> entry : futures.entrySet()) {
            WorkerJvm workerJvm = entry.getKey();
            CommandFuture future = entry.getValue();
            try {
                Object result = future.get(30, TimeUnit.SECONDS);
                results.add(result);
            } catch (TimeoutException e) {
                if(!future.getCommand().ignoreTimeout()) {
                    registerWorkerFailure(workerJvm, e);
                }
                throw e;
            } catch (ExecutionException e) {
                registerWorkerFailure(workerJvm,e);
                throw new FailureAlreadyThrownRuntimeException(e);
            }
        }
        return results;
    }

    private void registerWorkerFailure(WorkerJvm workerJvm, Exception e) {
        Failure failure = new Failure();
        failure.type = Failure.Type.WORKER_EXCEPTION;
        failure.message = e.getMessage();
        failure.agentAddress = getHostAddress();
        failure.workerAddress = workerJvm.memberAddress;
        failure.workerId = workerJvm.id;
        failure.testSuite = agent.getTestSuite();
        failure.cause = throwableToString(e);
        agent.getWorkerJvmFailureMonitor().publish(failure);
    }

    public void spawn(WorkerJvmSettings settings) throws Exception {
        this.lastUsedWorkerJvmSettings = settings;
        WorkerJvmLauncher launcher = new WorkerJvmLauncher(agent, workerJvms, settings);
        launcher.launch();
    }

    public void terminateWorkers() {
        log.info("Terminating workers");

        for (WorkerJvm jvm : new LinkedList<WorkerJvm>(workerJvms.values())) {
            terminateWorker(jvm);
        }

        log.info("Finished terminating workers");
    }

    public void terminateRandomWorker() {
        WorkerJvm randomWorker = getRandomWorkerOrNull();
        if (randomWorker == null) {
            log.warn("Attempt to terminating a random worker detected, but no worker is running.");
            return;
        }
        terminateWorker(randomWorker);
    }

    public void terminateWorker(final WorkerJvm jvm) {
        workerJvms.remove(jvm.id);

        Thread t = new Thread() {
            public void run() {
                try {
                    jvm.process.destroy(); //this sends SIGTERM on *nix
                    jvm.process.waitFor();
                } catch (Throwable e) {
                    log.fatal("Failed to destroy worker process: " + jvm);
                }
            }
        };

        t.start();
        try {
            t.join(WAIT_FOR_PROCESS_TERMINATION_TIMEOUT_MILLIS);
            if (t.isAlive()) {
                log.warn("WorkerJVM is still busy terminating: " + jvm);
            }
        } catch (Exception e) {
            log.fatal(e);
        }
    }

    private WorkerJvm getRandomWorkerWithClusterMemberOrNull() {
        List<WorkerJvm> jvmCollection = withoutMode(workerJvms.values(), WorkerJvm.Mode.CLIENT);
        if (jvmCollection.isEmpty()) {
            return null;
        }
        WorkerJvm[] workers = jvmCollection.toArray(new WorkerJvm[jvmCollection.size()]);
        return workers[random.nextInt(workers.length)];
    }

    private List<WorkerJvm> getAllWorkersWithClusterMembers() {
        return withoutMode(workerJvms.values(), WorkerJvm.Mode.CLIENT);
    }

    public List<WorkerJvm> withoutMode(Iterable<WorkerJvm> source, WorkerJvm.Mode mode) {
        List<WorkerJvm> result = new ArrayList<WorkerJvm>();
        for (WorkerJvm workerJvm : source) {
            if (!workerJvm.mode.equals(mode)) {
                result.add(workerJvm);
            }
        }
        return result;
    }

    private WorkerJvm getRandomWorkerOrNull() {
        Collection<WorkerJvm> jvmCollection = workerJvms.values();
        if (jvmCollection.isEmpty()) {
            return null;
        }
        WorkerJvm[] workers = jvmCollection.toArray(new WorkerJvm[jvmCollection.size()]);
        return workers[random.nextInt(workers.length)];
    }

    public void newMember() throws Exception {
        log.info("Adding a newMember");

        WorkerJvmSettings lastUsedWorkerJvmSettings = this.lastUsedWorkerJvmSettings;
        if (lastUsedWorkerJvmSettings == null) {
            log.warn("No lastUsedWorkerJvmSettings available");
            return;
        }

        WorkerJvmSettings settings = new WorkerJvmSettings(lastUsedWorkerJvmSettings);
        settings.memberWorkerCount = 1;
        settings.clientWorkerCount = 0;

        WorkerJvmLauncher launcher = new WorkerJvmLauncher(agent, workerJvms, settings);
        launcher.launch();
    }

    private class ClientSocketTask implements Runnable {
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
                WorkerJvm workerJvm = workerJvms.get(workerId);

                Object result = null;
                try {
                    if (workerJvm == null) {
                        log.warn("No worker JVM found for id: " + workerId);
                        result = new TerminateWorkerException();
                    } else {
                        workerJvm.lastSeen = System.currentTimeMillis();
                        if (SERVICE_POLL_WORK.equals(service)) {
                            List<CommandRequest> commands = new LinkedList<CommandRequest>();
                            workerJvm.commandQueue.drainTo(commands);
                            result = commands;
                        } else if (COMMAND_PUSH_RESPONSE.equals(service)) {
                            CommandResponse response = (CommandResponse) in.readObject();
                            //log.info("Received response: " + response.commandId);
                            CommandFuture f = futureMap.remove(response.commandId);
                            if (f != null) {
                                f.set(response.result);
                            } else {
                                log.fatal("No future found for commandId: " + response.commandId);
                            }
                        } else {
                            throw new RuntimeException("Unknown service:" + service);
                        }
                    }
                } catch (IOException e) {
                    throw e;
                } catch (Exception e) {
                    log.fatal("Failed to process serviceId:" + service, e);
                    result = e;
                }

                out.writeObject(result);
                out.flush();
                clientSocket.close();
            } catch (Exception e) {
                log.fatal(e);
            }
        }
    }

    private class AcceptorThread extends Thread {
        public AcceptorThread() {
            super("AcceptorThread");
        }

        public void run() {
            for (; ; ) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    if (log.isDebugEnabled()) {
                        log.debug("Accepted worker request from: " + clientSocket.getRemoteSocketAddress());
                    }
                    executor.execute(new ClientSocketTask(clientSocket));
                } catch (Throwable e) {
                    log.fatal(e);
                }
            }
        }
    }
}
