package com.hazelcast.stabilizer.agent.workerjvm;


import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.agent.Agent;
import com.hazelcast.stabilizer.agent.SpawnWorkerFailedException;
import com.hazelcast.stabilizer.worker.ClientWorker;
import com.hazelcast.stabilizer.worker.MemberWorker;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
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
import static java.util.Arrays.asList;

public class WorkerJvmLauncher {

    private final static Logger log = Logger.getLogger(WorkerJvmLauncher.class);

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
    private File testSuiteDir;

    public WorkerJvmLauncher(Agent agent, ConcurrentMap<String, WorkerJvm> workerJvms, WorkerJvmSettings settings) {
        this.settings = settings;
        this.workerJvms = workerJvms;
        this.agent = agent;
    }

    public void launch() throws Exception {
        hzFile = createHzConfigFile();
        clientHzFile = createClientHzConfigFile();

        testSuiteDir = agent.getTestSuiteDir();
        if (!testSuiteDir.exists()) {
            if (!testSuiteDir.mkdirs()) {
                throw new SpawnWorkerFailedException("Couldn't create testSuiteDir: " + testSuiteDir.getAbsolutePath());
            }
        }

        log.info("Spawning Worker JVM using settings: " + settings);
        spawn(settings.memberWorkerCount, "server");
        spawn(settings.clientWorkerCount, "client");
        spawn(settings.mixedWorkerCount, "mixed");
    }

    private void spawn(int count, String mode) throws Exception {
        log.info(format("Starting %s %s worker Java Virtual Machines", count, mode));

        for (int k = 0; k < count; k++) {
            WorkerJvm worker = startWorkerJvm(mode);
            workersInProgress.add(worker);
        }

        log.info(format("Finished starting %s %s worker Java Virtual Machines", count, mode));

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
        String javaHome = System.getProperty("java.home");
        if (javaHomePrinted.compareAndSet(false, true)) {
            log.info("java.home=" + javaHome);
        }

        return javaHome;
    }

    private WorkerJvm startWorkerJvm(String mode) throws IOException {
        String workerId = "worker-" + getHostAddress() + "-" + WORKER_ID_GENERATOR.incrementAndGet() + "-" + mode;
        File workerHome = new File(testSuiteDir, workerId);
        if (!workerHome.exists()) {
            if (!workerHome.mkdir()) {
                throw new SpawnWorkerFailedException("Could not create workerhome: " + workerHome.getAbsolutePath());
            }
        }

        String javaHome = getJavaHome(settings.javaVendor, settings.javaVersion);

        WorkerJvm workerJvm = new WorkerJvm(workerId);
        workerJvm.workerHome = workerHome;

        String[] args = buildArgs(workerJvm, mode);

        ProcessBuilder processBuilder = new ProcessBuilder(args)
                .directory(workerHome)
                .redirectErrorStream(true);

        Map<String, String> environment = processBuilder.environment();
        String path = javaHome + File.pathSeparator + "bin:" + environment.get("PATH");
        environment.put("PATH", path);
        environment.put("JAVA_HOME", javaHome);

        Process process = processBuilder.start();
        File logFile = new File(workerHome, "out.log");
        new WorkerJvmProcessOutputGobbler(process.getInputStream(), new FileOutputStream(logFile)).start();
        workerJvm.process = process;
        workerJvm.mode = WorkerJvm.Mode.valueOf(mode.toUpperCase());
        workerJvms.put(workerId, workerJvm);
        return workerJvm;
    }

    private String getClasspath() {
        File libDir = new File(agent.getTestSuiteDir(), "lib");
        return CLASSPATH + CLASSPATH_SEPARATOR + new File(libDir, "*").getAbsolutePath();
    }

    private List<String> getJvmOptions(WorkerJvmSettings settings, String mode) {
        String workerVmOptions;
        if ("client".equals(mode)) {
            workerVmOptions = settings.clientVmOptions;
        } else {
            workerVmOptions = settings.vmOptions;
        }

        String[] vmOptionsArray = new String[]{};
        if (workerVmOptions != null && !workerVmOptions.trim().isEmpty()) {
            vmOptionsArray = workerVmOptions.split("\\s+");
        }
        return asList(vmOptionsArray);
    }

    private String[] buildArgs(WorkerJvm workerJvm, String mode) {
        List<String> args = new LinkedList<String>();

        if ("perf".equals(settings.profiler)) {
            String[] perfSettings = settings.perfSettings.split("\\s+");
            // perf command always need to be in front of the java command.
            args.addAll(asList(perfSettings));
            args.add("java");
        } else if ("yourkit".equals(settings.profiler)) {
            args.add("java");
            String agentSetting = settings.yourkitConfig
                    .replace("${STABILIZER_HOME}", STABILIZER_HOME.getAbsolutePath())
                    .replace("${WORKER_HOME}", workerJvm.workerHome.getAbsolutePath());
            args.add(agentSetting);
        } else if ("hprof".equals(settings.profiler)) {
            args.add("java");
            args.add(settings.hprofSettings);
        } else {
            args.add("java");
        }

        args.add("-XX:OnOutOfMemoryError=\"\"touch worker.oome\"\"");
        args.add("-DSTABILIZER_HOME=" + STABILIZER_HOME);
        args.add("-Dhazelcast.logging.type=log4j");
        args.add("-DworkerId=" + workerJvm.id);
        args.add("-DworkerMode=" + mode);
        args.add("-Dlog4j.configuration=file:" + STABILIZER_HOME + File.separator + "conf" + File.separator + "worker-log4j.xml");
        args.add("-classpath");
        args.add(getClasspath());
        args.addAll(getJvmOptions(settings, mode));

        // if it is a client, we start the ClientWorker.
        if ("client".equals(mode)) {
            args.add(ClientWorker.class.getName());
        } else {
            args.add(MemberWorker.class.getName());
        }
        args.add(hzFile.getAbsolutePath());
        args.add(clientHzFile.getAbsolutePath());
        return args.toArray(new String[args.size()]);
    }

    private boolean hasExited(WorkerJvm workerJvm) {
        try {
            workerJvm.process.exitValue();
            return true;
        } catch (IllegalThreadStateException e) {
            return false;
        }
    }

    private void waitForWorkersStartup(List<WorkerJvm> workers, int workerTimeoutSec) throws InterruptedException {
        List<WorkerJvm> todo = new ArrayList<WorkerJvm>(workers);

        for (int l = 0; l < workerTimeoutSec; l++) {
            for (Iterator<WorkerJvm> it = todo.iterator(); it.hasNext(); ) {
                WorkerJvm jvm = it.next();

                if (hasExited(jvm)) {
                    String message = format("Startup failure: worker on host %s failed during startup, " +
                                    "check '%s/out.log' for more info",
                            getHostAddress(), jvm.workerHome
                    );
                    throw new SpawnWorkerFailedException(message);
                }

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

        workerTimeout(workerTimeoutSec, todo);
    }

    private void workerTimeout(int workerTimeoutSec, List<WorkerJvm> todo) {
        StringBuffer sb = new StringBuffer();
        sb.append("[");
        sb.append(todo.get(0).id);
        for (int l = 1; l < todo.size(); l++) {
            sb.append(",").append(todo.get(l).id);
        }
        sb.append("]");

        throw new SpawnWorkerFailedException(format("Timeout: workers %s of testsuite %s on host %s didn't start within %s seconds",
                sb, agent.getTestSuite().id, getHostAddress(),
                workerTimeoutSec));
    }

    private String readAddress(WorkerJvm jvm) {
        File file = new File(jvm.workerHome, "worker.address");
        if (!file.exists()) {
            return null;
        }

        String address = Utils.readObject(file);
        file.delete();
        return address;
    }
}
