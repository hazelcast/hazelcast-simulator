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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.Failure;
import com.hazelcast.stabilizer.FailureAlreadyThrownRuntimeException;
import com.hazelcast.stabilizer.JavaInstallation;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.worker.Worker;
import com.hazelcast.stabilizer.worker.WorkerVmLogger;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
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

    private final List<WorkerJvm> workerJvms = new CopyOnWriteArrayList<WorkerJvm>();
    private final Agent agent;
    private volatile HazelcastInstance workerClient;
    private volatile IExecutorService workerExecutor;
    private final AtomicBoolean javaHomePrinted = new AtomicBoolean();

    public WorkerJvmManager(Agent agent) {
        this.agent = agent;

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                for (WorkerJvm jvm : workerJvms) {
                    log.info("Destroying Worker : " + jvm.getId());
                    jvm.getProcess().destroy();
                }
            }
        });
    }

    public List executeOnWorkers(Callable task, String taskDescription) throws InterruptedException {
        Map<WorkerJvm, Future> futures = new HashMap<WorkerJvm, Future>();

        for (WorkerJvm workerJvm : getWorkerJvms()) {
            Member member = workerJvm.getMember();
            if (member == null) continue;

            Future future = getWorkerExecutor().submitToMember(task, member);
            futures.put(workerJvm, future);
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
                failure.workerAddress = workerJvm.getMember().getInetSocketAddress().getHostString();
                failure.workerId = workerJvm.getId();
                failure.testRecipe = agent.getTestRecipe();
                failure.cause = throwableToString(e);
                agent.getFailureMonitor().publish(failure);
                throw new FailureAlreadyThrownRuntimeException(e);
            }
        }
        return results;
    }

      public void cleanWorkersHome() throws IOException {
        for (File file : WORKERS_HOME.listFiles()) {
            Utils.delete(file);
        }
    }

    public IExecutorService getWorkerExecutor() {
        return workerExecutor;
    }

    public HazelcastInstance getWorkerClient() {
        return workerClient;
    }

    public List<WorkerJvm> getWorkerJvms() {
        return workerJvms;
    }

    public void spawn(WorkerVmSettings settings) throws Exception {
        log.info(format("Starting %s worker Java Virtual Machines using settings\n %s", settings.workerCount, settings));

        File workerHzFile = createHazelcastConfigFile(settings);

        List<WorkerJvm> workers = new LinkedList<WorkerJvm>();

        for (int k = 0; k < settings.workerCount; k++) {
            WorkerJvm worker = startWorkerJvm(settings, workerHzFile);
            Process process = worker.getProcess();
            String workerId = worker.getId();

            workers.add(worker);

            new WorkerVmLogger(workerId, process.getInputStream(), settings.trackLogging).start();
        }
        Config config = new XmlConfigBuilder(workerHzFile.getAbsolutePath()).build();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig()
                .setName(config.getGroupConfig().getName())
                .setPassword(config.getGroupConfig().getPassword());
        clientConfig.getNetworkConfig().addAddress("localhost:" + config.getNetworkConfig().getPort());

        workerClient = HazelcastClient.newHazelcastClient(clientConfig);
        workerExecutor = workerClient.getExecutorService(Worker.WORKER_EXECUTOR);

        waitForWorkersStartup(workers, settings.workerStartupTimeout);

        log.info(format("Finished starting %s worker Java Virtual Machines", settings.workerCount));
    }

    private File createHazelcastConfigFile(WorkerVmSettings settings) throws IOException {
        File workerHzFile = File.createTempFile("worker-hazelcast", "xml");
        workerHzFile.deleteOnExit();
        final String hzConfig = settings.hzConfig;

        StringBuffer members = new StringBuffer();
        HazelcastInstance agentHazelcastInstance = agent.getAgentHz();
        Cluster cluster = agentHazelcastInstance.getCluster();
        for (Member member : cluster.getMembers()) {
            String hostAddress = member.getSocketAddress().getAddress().getHostAddress();
            members.append("<member>").append(hostAddress).append(":5701").append("</member>\n");
        }

        String enhancedHzConfig = hzConfig.replace("<!--MEMBERS-->", members);
        Utils.writeText(enhancedHzConfig, workerHzFile);
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

    private WorkerJvm startWorkerJvm(WorkerVmSettings settings, File workerHzFile) throws IOException {
        String workerId = "worker-" + getHostAddress() + "-" + WORKER_ID_GENERATOR.incrementAndGet();

        String workerVmOptions = settings.vmOptions;
        String[] clientVmOptionsArray = new String[]{};
        if (workerVmOptions != null && !workerVmOptions.trim().isEmpty()) {
            clientVmOptionsArray = workerVmOptions.split("\\s+");
        }

        File workoutHome = agent.getWorkoutHome();
        String javaHome = getJavaHome(settings.javaVendor, settings.javaVersion);

        List<String> args = new LinkedList<String>();
        args.add("java");
        args.add(format("-XX:OnOutOfMemoryError=\"\"touch %s.oome\"\"", workerId));
        args.add("-DSTABILIZER_HOME=" + getStablizerHome());
        args.add("-Dhazelcast.logging.type=log4j");
        args.add("-DworkerId=" + workerId);
        args.add("-Dlog4j.configuration=file:" + STABILIZER_HOME + File.separator + "conf" + File.separator + "worker-log4j.xml");
        args.add("-classpath");

        File libDir = new File(agent.getWorkoutHome(), "lib");
        String s = CLASSPATH + CLASSPATH_SEPARATOR + new File(libDir, "*").getAbsolutePath();
        args.add(s);

        args.addAll(Arrays.asList(clientVmOptionsArray));
        args.add(Worker.class.getName());
        args.add(workerId);
        args.add(workerHzFile.getAbsolutePath());

        ProcessBuilder processBuilder = new ProcessBuilder(args.toArray(new String[args.size()]))
                .directory(workoutHome)
                .redirectErrorStream(true);

        Map<String, String> environment = processBuilder.environment();
        String path = javaHome + File.pathSeparator + "bin:" + environment.get("PATH");
        environment.put("PATH", path);
        environment.put("JAVA_HOME", javaHome);

        Process process = processBuilder.start();
        final WorkerJvm workerJvm = new WorkerJvm(workerId, process);
        workerJvms.add(workerJvm);
        return workerJvm;
    }

    private void waitForWorkersStartup(List<WorkerJvm> workers, int workerTimeoutSec) throws InterruptedException {
        List<WorkerJvm> todo = new ArrayList<WorkerJvm>(workers);

        for (int l = 0; l < workerTimeoutSec; l++) {
            for (Iterator<WorkerJvm> it = todo.iterator(); it.hasNext(); ) {
                WorkerJvm jvm = it.next();

                InetSocketAddress address = readAddress(jvm);

                if (address != null) {
                    Member member = null;
                    for (Member m : workerClient.getCluster().getMembers()) {
                        if (m.getInetSocketAddress().equals(address)) {
                            member = m;
                            break;
                        }
                    }

                    if (member != null) {
                        it.remove();
                        jvm.setMember(member);
                        log.info(format("Worker: %s Started %s of %s",
                                jvm.getId(), workers.size() - todo.size(), workers.size()));
                    }
                }
            }

            if (todo.isEmpty())
                return;

            Utils.sleepSeconds(1);
        }

        StringBuffer sb = new StringBuffer();
        sb.append("[");
        sb.append(todo.get(0).getId());
        for (int l = 1; l < todo.size(); l++) {
            sb.append(",").append(todo.get(l).getId());
        }
        sb.append("]");

        throw new RuntimeException(format("Timeout: workers %s of workout %s on host %s didn't start within %s seconds",
                sb, agent.getWorkout().id, agent.getAgentHz().getCluster().getLocalMember().getInetSocketAddress(),
                workerTimeoutSec));
    }

    private InetSocketAddress readAddress(WorkerJvm jvm) {
        File workoutHome = agent.getWorkoutHome();

        File file = new File(workoutHome, jvm.getId() + ".address");
        if (!file.exists()) {
            return null;
        }

        return (InetSocketAddress) Utils.readObject(file);
    }

    public void terminateWorkers() {
        log.info("Terminating workers");

        if (workerClient != null) {
            workerClient.getLifecycleService().shutdown();
        }

        List<WorkerJvm> workers = new LinkedList<WorkerJvm>(workerJvms);
        workerJvms.clear();

        for (WorkerJvm jvm : workers) {
            jvm.getProcess().destroy();
        }

        for (WorkerJvm jvm : workers) {
            int exitCode = 0;
            try {
                exitCode = jvm.getProcess().waitFor();
            } catch (InterruptedException e) {
            }

            if (exitCode != 0) {
                log.info(format("worker process %s exited with exit code: %s", jvm.getId(), exitCode));
            }
        }

        log.info("Finished terminating workers");
    }

    public void destroy(WorkerJvm jvm) {
        jvm.getProcess().destroy();
        try {
            jvm.getProcess().waitFor();
        } catch (InterruptedException e) {
        }
        workerJvms.remove(jvm);
    }

    public WorkerJvm getWorker(String workerId) {
        for (WorkerJvm worker : workerJvms) {
            if (workerId.equals(worker.getId())) {
                return worker;
            }
        }
        return null;
    }
}
