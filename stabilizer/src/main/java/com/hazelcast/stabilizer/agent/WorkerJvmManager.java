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
import com.hazelcast.stabilizer.worker.testcommands.TestResponse;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
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
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.stabilizer.Utils.getHostAddress;
import static com.hazelcast.stabilizer.Utils.getStablizerHome;
import static com.hazelcast.stabilizer.Utils.throwableToString;
import static java.lang.String.format;

public class WorkerJvmManager {

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
        new PollThread().start();

        serverSocket = new ServerSocket(10000, 0, InetAddress.getByName(null));

        new Thread() {
            public void run() {
                for (; ; ) {
                    try {
                        Socket clientSocket = serverSocket.accept();
                        log.info("Received accept from " + clientSocket.getRemoteSocketAddress());

                        ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
                        out.flush();

                        InputStream inputStream = clientSocket.getInputStream();
                        ObjectInputStream in = new ObjectInputStream(inputStream);

                        log.info("Waiting for confirmation of worker process");
                        String id = (String) in.readObject();
                        WorkerJvm jvm = workerJvms.get(id);
                        log.info("JVM: " + jvm);
                        jvm.out = out;
                        jvm.in = in;
                        jvm.in2 = inputStream;
                        jvm.socket = clientSocket;
                    } catch (Exception e) {
                        log.severe(e);
                    }
                }
            }
        }.start();
    }

    public void cleanWorkersHome() throws IOException {
        for (File file : WORKERS_HOME.listFiles()) {
            Utils.delete(file);
        }
    }

    public Collection<WorkerJvm> getWorkerJvms() {
        return workerJvms.values();
    }

    public List executeOnWorkers(TestCommand testCommand, String taskDescription) throws InterruptedException {
        Map<WorkerJvm, Future> futures = new HashMap<WorkerJvm, Future>();

        for (WorkerJvm workerJvm : getWorkerJvms()) {
            TestCommandFuture future = new TestCommandFuture();
            TestCommandRequest request = new TestCommandRequest();
            request.id = requestIdGenerator.incrementAndGet();
            request.task = testCommand;
            futureMap.put(request.id, future);
            try {
                ObjectOutputStream out = workerJvm.out;
                if (out == null) {
                    log.severe("jvm: " + workerJvm.id + " has no out");
                } else {
                    out.writeObject(request);
                    out.flush();
                    log.info("Successfully send " + testCommand + " to worker: " + workerJvm.id);
                }
                futures.put(workerJvm, future);
            } catch (IOException e) {
                future.set(e);
                throw new RuntimeException(e);
            }
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
                failure.message = taskDescription;
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

                    //hack
                    for (; ; ) {
                        if (jvm.out != null && jvm.in != null) {
                            break;
                        }

                        log.info("JVM.in and out not yet set");

                        Thread.sleep(100);
                    }
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
            Socket socket = jvm.socket;
            if (socket != null) {
                Utils.closeQuietly(socket);
            }
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

    private class PollThread extends Thread {
        public PollThread() {
            super("PollThread");
        }

        public void run() {
            for (; ; ) {
                for (WorkerJvm workerJvm : workerJvms.values()) {
                    poll(workerJvm);
                }
                Utils.sleepSeconds(1);
            }
        }

        private void poll(WorkerJvm workerJvm) {
            ObjectInputStream in = workerJvm.in;

            //could be that the in is not yet set.
            if (in == null) {
                return;
            }

            try {
                if (workerJvm.in2.available() > 0) {
                    log.info("Waiting for object");
                    TestResponse response = (TestResponse) in.readObject();
                    log.info("Received response: " + response.commandId);
                    TestCommandFuture f = futureMap.remove(response.commandId);
                    if (f != null) {
                        f.set(response.result);
                    }
                }
            } catch (Exception e) {
                log.severe("Failed to poll result for jvm:" + workerJvm.id, e);
            }
        }
    }
}
