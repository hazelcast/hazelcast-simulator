package com.hazelcast.stabilizer.agent.workerjvm;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.agent.Agent;
import com.hazelcast.stabilizer.agent.JavaInstallation;
import com.hazelcast.stabilizer.worker.Worker;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.stabilizer.Utils.getHostAddress;
import static com.hazelcast.stabilizer.Utils.getStablizerHome;
import static com.hazelcast.stabilizer.Utils.writeText;
import static java.lang.String.format;

public class WorkerJvmLauncher {


    private final static ILogger log = Logger.getLogger(WorkerJvmLauncher.class);

    private final AtomicBoolean javaHomePrinted = new AtomicBoolean();
    private final static String CLASSPATH = System.getProperty("java.class.path");
    private final static File STABILIZER_HOME = getStablizerHome();
    private final static String CLASSPATH_SEPARATOR = System.getProperty("path.separator");
    private final static AtomicLong WORKER_ID_GENERATOR = new AtomicLong();

    private final WorkerJvmSettings settings;
    private final Agent agent;
    private final ConcurrentMap<String, WorkerJvm> workerJvms;
    private File hzFile;
    private File clientHzFile;
    private final List<WorkerJvm> workersInProgress = new LinkedList<WorkerJvm>();

    public WorkerJvmLauncher(Agent agent, ConcurrentMap<String, WorkerJvm> workerJvms, WorkerJvmSettings settings) {
        this.settings = settings;
        this.workerJvms = workerJvms;
        this.agent = agent;
    }

    public void launch() throws Exception {
        hzFile = createHzConfigFile();
        clientHzFile = createClientHzConfigFile();

        spawn(settings.memberWorkerCount, "server");
        spawn(settings.clientWorkerCount, "client");
        spawn(settings.mixedWorkerCount, "mixed");
    }

    private void spawn(int count, String mode) throws Exception {
        log.info(format("Starting %s %s worker Java Virtual Machines using settings\n %s", count, mode, settings));

        for (int k = 0; k < count; k++) {
            WorkerJvm worker = startWorkerJvm(mode);
            Process process = worker.process;
            String workerId = worker.id;
            workersInProgress.add(worker);
            //we need to consume the inputsteam.
            new WorkerVmInputStreamConsumer(workerId, process.getInputStream(), settings.trackLogging).start();
        }

        log.info(format("Finished starting %s %s worker Java Virtual Machines using settings\n %s", count, mode, settings));

        waitForWorkersStartup(workersInProgress, settings.workerStartupTimeout);
        workersInProgress.clear();
    }

    private File createHzConfigFile() throws IOException {
        File hzConfigFile = File.createTempFile("hazelcast", "xml");
        hzConfigFile.deleteOnExit();
        writeText(settings.hzConfig, hzConfigFile);
        return hzConfigFile;
    }

    private File createClientHzConfigFile() throws IOException {
        File clientHzConfigFile = File.createTempFile("client-hazelcast", "xml");
        clientHzConfigFile.deleteOnExit();
        writeText(settings.clientHzConfig, clientHzConfigFile);
        return clientHzConfigFile;
    }

    private String getJavaHome(String javaVendor, String javaVersion) {
        JavaInstallation installation = agent.getJavaInstallationRepository().get(javaVendor, javaVersion);
        if (installation != null) {
            return installation.getJavaHome();
        }

        //nothing is found so we are going to make use of a default.
        String javaHome = System.getProperty("java.home");
        if (javaHomePrinted.compareAndSet(false, true)) {
            log.info("java.home=" + javaHome);
        }

        return javaHome;
    }

    private WorkerJvm startWorkerJvm(String mode) throws IOException {
        String workerId = "worker-" + getHostAddress() + "-" + WORKER_ID_GENERATOR.incrementAndGet();

        File testSuiteHome = agent.getTestSuiteDir();
        testSuiteHome.mkdirs();

        String javaHome = getJavaHome(settings.javaVendor, settings.javaVersion);

        WorkerJvm workerJvm = new WorkerJvm(workerId);

        String[] args = buildArgs(workerId, mode);

        ProcessBuilder processBuilder = new ProcessBuilder(args)
                .directory(testSuiteHome)
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

    private String getClasspath() {
        File libDir = new File(agent.getTestSuiteDir(), "lib");
        return CLASSPATH + CLASSPATH_SEPARATOR + new File(libDir, "*").getAbsolutePath();
    }

    private List<String> getJvmOptions(WorkerJvmSettings settings) {
        String workerVmOptions = settings.vmOptions;
        String[] clientVmOptionsArray = new String[]{};
        if (workerVmOptions != null && !workerVmOptions.trim().isEmpty()) {
            clientVmOptionsArray = workerVmOptions.split("\\s+");
        }
        return Arrays.asList(clientVmOptionsArray);
    }

    private String[] buildArgs(String workerId, String mode) {
        List<String> args = new LinkedList<String>();
        args.add("java");
        args.add(format("-XX:OnOutOfMemoryError=\"\"touch %s.oome\"\"", workerId));
        args.add("-DSTABILIZER_HOME=" + STABILIZER_HOME);
        args.add("-Dhazelcast.logging.type=log4j");
        args.add("-DworkerId=" + workerId);
        args.add("-DworkerMode=" + mode);
        args.add("-Dlog4j.configuration=file:" + STABILIZER_HOME + File.separator + "conf" + File.separator + "worker-log4j.xml");
        args.add("-classpath");
        args.add(getClasspath());
        args.addAll(getJvmOptions(settings));
        args.add(Worker.class.getName());
        args.add(hzFile.getAbsolutePath());
        args.add(clientHzFile.getAbsolutePath());
        return args.toArray(new String[args.size()]);
    }

    private void waitForWorkersStartup(List<WorkerJvm> workers, int workerTimeoutSec) throws InterruptedException {
        List<WorkerJvm> todo = new ArrayList<WorkerJvm>(workers);

        for (int l = 0; l < workerTimeoutSec; l++) {
            for (Iterator<WorkerJvm> it = todo.iterator(); it.hasNext(); ) {
                WorkerJvm jvm = it.next();

                String address = readAddress(jvm);

                if (address != null) {
                    jvm.memberAddress = address;

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

        throw new RuntimeException(format("Timeout: workers %s of testsuite %s on host %s didn't start within %s seconds",
                sb, agent.getTestSuite().id, getHostAddress(),
                workerTimeoutSec));
    }

    private String readAddress(WorkerJvm jvm) {
        File testSuiteHome = agent.getTestSuiteDir();

        File file = new File(testSuiteHome, jvm.id + ".address");
        if (!file.exists()) {
            return null;
        }

        return Utils.readObject(file);
    }
}
