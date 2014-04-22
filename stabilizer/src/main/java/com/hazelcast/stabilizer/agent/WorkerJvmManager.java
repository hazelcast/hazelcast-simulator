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
package com.hazelcast.stabilizer.agent;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.tests.Failure;
import com.hazelcast.stabilizer.worker.Worker;
import com.hazelcast.stabilizer.worker.testcommands.TestCommand;
import com.hazelcast.stabilizer.worker.testcommands.TestCommandRequest;
import com.hazelcast.stabilizer.worker.testcommands.TestCommandResponse;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.stabilizer.Utils.getHostAddress;
import static com.hazelcast.stabilizer.Utils.getStablizerHome;
import static com.hazelcast.stabilizer.Utils.throwableToString;
import static java.lang.String.format;

public class WorkerJvmManager {

    public final static String SERVICE_POLL_WORK = "poll";
    public final static String COMMAND_PUSH_RESPONSE = "push";

    private final static ILogger log = Logger.getLogger(WorkerJvmManager.class);
    private final static String CLASSPATH = System.getProperty("java.class.path");
    private final static File STABILIZER_HOME = getStablizerHome();
    private final static String CLASSPATH_SEPARATOR = System.getProperty("path.separator");
    private final static AtomicLong WORKER_ID_GENERATOR = new AtomicLong();
    public final static File WORKERS_HOME = new File(getStablizerHome(), "workers");

    private final ConcurrentMap<String, WorkerJvm> workerJvms = new ConcurrentHashMap<String, WorkerJvm>();
    private final Agent agent;
    private final AtomicBoolean javaHomePrinted = new AtomicBoolean();

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
        serverSocket = new ServerSocket(10000, 0, InetAddress.getByName(null));

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
                Object result = future.get();
                results.add(result);
            } catch (ExecutionException e) {
                Failure failure = new Failure();
                failure.message = e.getMessage();
                failure.agentAddress = getHostAddress();
                failure.workerAddress = workerJvm.memberAddress;
                failure.workerId = workerJvm.id;
                failure.testRecipe = agent.getTestRecipe();
                failure.cause = throwableToString(e);
                agent.getWorkerJvmFailureMonitor().publish(failure);
                throw new FailureAlreadyThrownRuntimeException(e);
            }
        }
        return results;
    }


    public void spawn(WorkerJvmSettings settings) throws Exception {
        log.info(format("Starting %s worker Java Virtual Machines using settings\n %s", settings.workerCount, settings));

        File workerHzFile = createHazelcastConfigFile(settings);

        List<WorkerJvm> workers = new LinkedList<WorkerJvm>();

        for (int k = 0; k < settings.workerCount; k++) {
            WorkerJvm worker = startWorkerJvm(settings, workerHzFile);

            Process process = worker.process;
            String workerId = worker.id;

            workers.add(worker);

            new WorkerVmLogger(workerId, process.getInputStream(), settings.trackLogging).start();
        }
        waitForWorkersStartup(workers, settings.workerStartupTimeout);

        log.info(format("Finished starting %s worker Java Virtual Machines", settings.workerCount));
    }

    private File createHazelcastConfigFile(WorkerJvmSettings settings) throws IOException {
        File workerHzFile = File.createTempFile("worker-hazelcast", "xml");
        workerHzFile.deleteOnExit();
        Utils.writeText(settings.hzConfig, workerHzFile);
        return workerHzFile;
    }

    private String getJavaHome(String javaVendor, String javaVersion) {
        JavaInstallation installation = agent.getJavaInstallationRepository().get(javaVendor, javaVersion);
        if (installation != null) {
            //todo: we should send a signal
            return installation.getJavaHome();
        }

        //nothing is found so we are going to make use of a default.
        String javaHome = System.getProperty("java.home");
        if (javaHomePrinted.compareAndSet(false, true)) {
            log.info("java.home=" + javaHome);
        }

        return javaHome;
    }

    private WorkerJvm startWorkerJvm(WorkerJvmSettings settings, File workerHzFile) throws IOException {
        String workerId = "worker-" + getHostAddress() + "-" + WORKER_ID_GENERATOR.incrementAndGet();

        File workoutHome = agent.getWorkoutHome();
        workoutHome.mkdirs();

        String javaHome = getJavaHome(settings.javaVendor, settings.javaVersion);

        WorkerJvm workerJvm = new WorkerJvm(workerId);

        String[] args = buildArgs(settings, workerHzFile, workerId);

        ProcessBuilder processBuilder = new ProcessBuilder(args)
                .directory(workoutHome)
                .redirectErrorStream(true);

        Map<String, String> environment = processBuilder.environment();
        String path = javaHome + File.pathSeparator + "bin:" + environment.get("PATH");
        environment.put("PATH", path);
        environment.put("JAVA_HOME", javaHome);

        Process process = processBuilder.start();
        workerJvm.process = process;
        workerJvms.put(workerId, workerJvm);
        return workerJvm;
    }

    private String[] buildArgs(WorkerJvmSettings settings, File workerHzFile, String workerId) {
        List<String> args = new LinkedList<String>();
        args.add("java");
        args.add(format("-XX:OnOutOfMemoryError=\"\"touch %s.oome\"\"", workerId));
        args.add("-DSTABILIZER_HOME=" + STABILIZER_HOME);
        args.add("-Dhazelcast.logging.type=log4j");
        args.add("-DworkerId=" + workerId);
        args.add("-Dlog4j.configuration=file:" + STABILIZER_HOME + File.separator + "conf" + File.separator + "worker-log4j.xml");
        args.add("-classpath");

        File libDir = new File(agent.getWorkoutHome(), "lib");
        String s = CLASSPATH + CLASSPATH_SEPARATOR + new File(libDir, "*").getAbsolutePath();
        args.add(s);

        args.addAll(getClientVmOptions(settings));
        args.add(Worker.class.getName());
        args.add(workerId);
        args.add(workerHzFile.getAbsolutePath());
        return args.toArray(new String[args.size()]);
    }

    private List<String> getClientVmOptions(WorkerJvmSettings settings) {
        String workerVmOptions = settings.vmOptions;
        String[] clientVmOptionsArray = new String[]{};
        if (workerVmOptions != null && !workerVmOptions.trim().isEmpty()) {
            clientVmOptionsArray = workerVmOptions.split("\\s+");
        }
        return Arrays.asList(clientVmOptionsArray);
    }

    private void waitForWorkersStartup(List<WorkerJvm> workers, int workerTimeoutSec) throws InterruptedException {
        List<WorkerJvm> todo = new ArrayList<WorkerJvm>(workers);

        for (int l = 0; l < workerTimeoutSec; l++) {
            for (Iterator<WorkerJvm> it = todo.iterator(); it.hasNext(); ) {
                WorkerJvm jvm = it.next();

                InetSocketAddress address = readAddress(jvm);

                if (address != null) {
                    jvm.memberAddress = address.getAddress().getHostAddress();

                    it.remove();
                    log.info(format("Worker: %s Started %s of %s",
                            jvm.id, workers.size() - todo.size(), workers.size()));
                }
            }

            if (todo.isEmpty()) {
                return;
            }

            Utils.sleepSeconds(1);
        }

        StringBuffer sb = new StringBuffer();
        sb.append("[");
        sb.append(todo.get(0).id);
        for (int l = 1; l < todo.size(); l++) {
            sb.append(",").append(todo.get(l).id);
        }
        sb.append("]");

        throw new RuntimeException(format("Timeout: workers %s of workout %s on host %s didn't start within %s seconds",
                sb, agent.getWorkout().id, getHostAddress(),
                workerTimeoutSec));
    }

    private InetSocketAddress readAddress(WorkerJvm jvm) {
        File workoutHome = agent.getWorkoutHome();

        File file = new File(workoutHome, jvm.id + ".address");
        if (!file.exists()) {
            return null;
        }

        return (InetSocketAddress) Utils.readObject(file);
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
