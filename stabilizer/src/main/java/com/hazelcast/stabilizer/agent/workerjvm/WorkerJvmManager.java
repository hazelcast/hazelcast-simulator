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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.agent.Agent;
import com.hazelcast.stabilizer.agent.FailureAlreadyThrownRuntimeException;
import com.hazelcast.stabilizer.agent.TestCommandFuture;
import com.hazelcast.stabilizer.tests.Failure;
import com.hazelcast.stabilizer.worker.testcommands.TestCommand;
import com.hazelcast.stabilizer.worker.testcommands.TestCommandRequest;
import com.hazelcast.stabilizer.worker.testcommands.TestCommandResponse;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.stabilizer.Utils.getHostAddress;
import static com.hazelcast.stabilizer.Utils.getStablizerHome;
import static com.hazelcast.stabilizer.Utils.throwableToString;
import static java.lang.String.format;

public class WorkerJvmManager {

    public final static String SERVICE_POLL_WORK = "poll";
    public final static String COMMAND_PUSH_RESPONSE = "push";

    private final static ILogger log = Logger.getLogger(WorkerJvmManager.class);
    public final static File WORKERS_HOME = new File(getStablizerHome(), "workers");
    public static final int PORT = 9001;

    private final ConcurrentMap<String, WorkerJvm> workerJvms = new ConcurrentHashMap<String, WorkerJvm>();
    private final Agent agent;

    private final ConcurrentMap<Long, TestCommandFuture> futureMap = new ConcurrentHashMap<Long, TestCommandFuture>();
    private final AtomicLong requestIdGenerator = new AtomicLong(0);

    private ServerSocket serverSocket;
    private final Executor executor = Executors.newFixedThreadPool(20);

    public WorkerJvmManager(Agent agent) {
        this.agent = agent;

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                for (WorkerJvm jvm : workerJvms.values()) {
                    log.info("Destroying Worker : " + jvm.id);
                    jvm.process.destroy();
                }
            }
        });
    }

    public void start() throws Exception {
        serverSocket = new ServerSocket(PORT, 0, InetAddress.getByName(null));

        log.info("Started Agent Work JVM Service on :" + serverSocket.getInetAddress().getHostAddress()+":"+PORT);

        new AcceptorThread().start();
    }

    public void cleanWorkersHome() throws IOException {
        for (File file : WORKERS_HOME.listFiles()) {
            Utils.delete(file);
        }
    }

    public Collection<WorkerJvm> getWorkerJvms() {
        return workerJvms.values();
    }

    public Object executeOnSingleWorker(TestCommand testCommand) throws Exception {
        Collection<WorkerJvm> workers = new LinkedList<WorkerJvm>(workerJvms.values());
        if (workers.isEmpty()) {
            throw new RuntimeException("No worker JVM's found");
        }
        List list = executeOnWorkers(testCommand, workers);
        return list.get(0);
    }

    public List executeOnAllWorkers(TestCommand testCommand) throws Exception {
        return executeOnWorkers(testCommand, workerJvms.values());
    }

    private List executeOnWorkers(TestCommand testCommand, Collection<WorkerJvm> workers) throws Exception {
        Map<WorkerJvm, Future> futures = new HashMap<WorkerJvm, Future>();

        for (WorkerJvm workerJvm : workers) {
            TestCommandFuture future = new TestCommandFuture();
            TestCommandRequest request = new TestCommandRequest();
            request.id = requestIdGenerator.incrementAndGet();
            request.task = testCommand;
            futureMap.put(request.id, future);
            workerJvm.commandQueue.add(request);
        }

        List results = new LinkedList();
        for (Map.Entry<WorkerJvm, Future> entry : futures.entrySet()) {
            WorkerJvm workerJvm = entry.getKey();
            Future future = entry.getValue();
            try {
                Object result = future.get(30, TimeUnit.SECONDS);
                results.add(result);
            } catch (ExecutionException e) {
                Failure failure = new Failure();
                failure.message = e.getMessage();
                failure.agentAddress = getHostAddress();
                failure.workerAddress = workerJvm.memberAddress;
                failure.workerId = workerJvm.id;
                failure.testCase = agent.getTestCase();
                failure.cause = throwableToString(e);
                agent.getWorkerJvmFailureMonitor().publish(failure);
                throw new FailureAlreadyThrownRuntimeException(e);
            }
        }
        return results;
    }

    public void spawn(WorkerJvmSettings settings) throws Exception {
        WorkerJvmLauncher launcher = new WorkerJvmLauncher(agent, workerJvms, settings);
        launcher.launch();
    }

    public void terminateWorkers() {
        log.info("Terminating workers");

        List<WorkerJvm> workers = new LinkedList<WorkerJvm>(workerJvms.values());
        workerJvms.clear();

        for (WorkerJvm jvm : workers) {
            jvm.process.destroy();
        }

        for (WorkerJvm jvm : workers) {
            int exitCode = 0;
            try {
                exitCode = jvm.process.waitFor();
            } catch (InterruptedException e) {
            }

            if (exitCode != 0) {
                log.info(format("worker process %s exited with exit code: %s", jvm.id, exitCode));
            }
        }

        log.info("Finished terminating workers");
    }

    public void terminateWorker(WorkerJvm jvm) {
        jvm.process.destroy();
        try {
            jvm.process.waitFor();
        } catch (InterruptedException e) {
        }
        workerJvms.remove(jvm);
    }

    public WorkerJvm getWorker(String workerId) {
        return workerJvms.get(workerId);
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
                if (workerId == null) {
                    log.warning("No worker JVM found for id: " + workerId);
                }

                Object result = null;
                try {
                    if (SERVICE_POLL_WORK.equals(service)) {
                        List<TestCommandRequest> commands = new LinkedList<TestCommandRequest>();
                        workerJvm.commandQueue.drainTo(commands);
                        result = commands;
                    } else if (COMMAND_PUSH_RESPONSE.equals(service)) {
                        TestCommandResponse response = (TestCommandResponse) in.readObject();
                        log.info("Received response: " + response.commandId);
                        TestCommandFuture f = futureMap.remove(response.commandId);
                        if (f != null) {
                            f.set(response.result);
                        } else {
                            log.severe("No future found for commandId: " + response.commandId);
                        }
                    } else {
                        throw new RuntimeException("Unknown service:" + service);
                    }
                } catch (IOException e) {
                    throw e;
                } catch (Exception e) {
                    result = e;
                }

                out.writeObject(result);
                out.flush();
                clientSocket.close();
            } catch (Exception e) {
                log.severe(e);
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
                    log.info("Accepted client request from: " + clientSocket.getRemoteSocketAddress());
                    executor.execute(new ClientSocketTask(clientSocket));
                } catch (IOException e) {
                    log.severe(e);
                }
            }
        }
    }
}
