package com.hazelcast.simulator.agent.workerjvm;

import com.hazelcast.simulator.agent.Agent;
import com.hazelcast.simulator.agent.SpawnWorkerFailedException;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.provisioner.Bash;
import com.hazelcast.simulator.worker.ClientWorker;
import com.hazelcast.simulator.worker.MemberWorker;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.utils.CommonUtils.getHostAddress;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.FileUtils.readObject;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static java.lang.String.format;
import static java.util.Arrays.asList;

public class WorkerJvmLauncher {

    private static final Logger LOGGER = Logger.getLogger(WorkerJvmLauncher.class);

    private static final String CLASSPATH = System.getProperty("java.class.path");
    private static final File SIMULATOR_HOME = getSimulatorHome();
    private static final String CLASSPATH_SEPARATOR = System.getProperty("path.separator");
    private static final AtomicLong WORKER_ID_GENERATOR = new AtomicLong();
    private static final String WORKERS_PATH = getSimulatorHome().getAbsolutePath() + "/workers";

    private final AtomicBoolean javaHomePrinted = new AtomicBoolean();
    private final WorkerJvmSettings settings;
    private final SimulatorProperties props = new SimulatorProperties();
    private final Bash bash = new Bash(props);
    private final Agent agent;
    private final ConcurrentMap<String, WorkerJvm> workerJVMs;
    private File hzFile;
    private File clientHzFile;
    private File log4jFile;
    private final List<WorkerJvm> workersInProgress = new LinkedList<WorkerJvm>();
    private File testSuiteDir;

    public WorkerJvmLauncher(Agent agent, ConcurrentMap<String, WorkerJvm> workerJVMs, WorkerJvmSettings settings) {
        this.settings = settings;
        this.workerJVMs = workerJVMs;
        this.agent = agent;
    }

    public void launch() throws Exception {
        hzFile = createTmpXmlFile("hazelcast", settings.hzConfig);
        clientHzFile = createTmpXmlFile("client-hazelcast", settings.clientHzConfig);
        log4jFile = createTmpXmlFile("worker-log4j", settings.log4jConfig);

        testSuiteDir = agent.getTestSuiteDir();
        if (!testSuiteDir.exists()) {
            if (!testSuiteDir.mkdirs()) {
                throw new SpawnWorkerFailedException("Couldn't create testSuiteDir: " + testSuiteDir.getAbsolutePath());
            }
        }

        LOGGER.info("Spawning Worker JVM using settings: " + settings);
        spawn(settings.memberWorkerCount, "server");
        spawn(settings.clientWorkerCount, "client");
    }

    private void spawn(int count, String mode) throws Exception {
        LOGGER.info(format("Starting %s %s worker Java Virtual Machines", count, mode));

        for (int k = 0; k < count; k++) {
            WorkerJvm worker = startWorkerJvm(mode);
            workersInProgress.add(worker);
        }

        LOGGER.info(format("Finished starting %s %s worker Java Virtual Machines", count, mode));

        waitForWorkersStartup(workersInProgress, settings.workerStartupTimeout);
        workersInProgress.clear();
    }

    private File createTmpXmlFile(String name, String content) throws IOException {
        File tmpXmlFile = File.createTempFile(name, ".xml");
        tmpXmlFile.deleteOnExit();
        writeText(content, tmpXmlFile);

        return tmpXmlFile;
    }

    @SuppressWarnings("unused")
    private String getJavaHome(String javaVendor, String javaVersion) {
        String javaHome = System.getProperty("java.home");
        if (javaHomePrinted.compareAndSet(false, true)) {
            LOGGER.info("java.home=" + javaHome);
        }

        return javaHome;
    }

    private WorkerJvm startWorkerJvm(String mode) throws IOException {
        String workerId = "worker-" + getHostAddress() + "-" + WORKER_ID_GENERATOR.incrementAndGet() + "-" + mode;
        File workerHome = new File(testSuiteDir, workerId);
        ensureExistingDirectory(workerHome);

        String javaHome = getJavaHome(settings.javaVendor, settings.javaVersion);

        WorkerJvm workerJvm = new WorkerJvm(workerId);
        workerJvm.workerHome = workerHome;

        generateWorkerStartScript(mode, workerJvm);

        ProcessBuilder processBuilder = new ProcessBuilder(new String[]{"bash", "worker.sh"})
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
        copyResourcesToWorkerId(workerId);
        workerJVMs.put(workerId, workerJvm);
        return workerJvm;
    }

    private void copyResourcesToWorkerId(String workerId) throws IOException {
        final String testSuiteId = agent.getTestSuite().id;
        File uploadDirectory = new File(WORKERS_PATH + "/" + testSuiteId + "/upload/");
        if (!uploadDirectory.exists()) {
            LOGGER.debug("Skip copying upload directory to workers since no upload directory was found");
            return;
        }
        String cpCommand = format("cp -rfv %s/%s/upload/* %s/%s/%s/",
                WORKERS_PATH,
                testSuiteId,
                WORKERS_PATH,
                testSuiteId,
                workerId);
        bash.execute(cpCommand);
        LOGGER.info(format("Finished copying '+%s+' to worker", WORKERS_PATH));
    }

    private void generateWorkerStartScript(String mode, WorkerJvm workerJvm) {
        String[] args = buildArgs(workerJvm, mode);
        File startScript = new File(workerJvm.workerHome, "worker.sh");

        StringBuilder sb = new StringBuilder("#!/bin/bash\n");
        for (String arg : args) {
            sb.append(arg).append(" ");
        }
        //sb.append(" > sysout.log");
        sb.append("\n");

        writeText(sb.toString(), startScript);
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

        String numaCtl = settings.numaCtl;
        if (!"none".equals(numaCtl)) {
            args.add(numaCtl);
        }

        String profiler = settings.profiler;
        if ("perf".equals(profiler)) {
            // perf command always need to be in front of the java command.
            args.add(settings.perfSettings);
            args.add("java");
        } else if ("vtune".equals(profiler)) {
            // vtune command always need to be in front of the java command.
            args.add(settings.vtuneSettings);
            args.add("java");
        } else if ("yourkit".equals(profiler)) {
            args.add("java");
            String agentSetting = settings.yourkitConfig
                    .replace("${SIMULATOR_HOME}", SIMULATOR_HOME.getAbsolutePath())
                    .replace("${WORKER_HOME}", workerJvm.workerHome.getAbsolutePath());
            args.add(agentSetting);
        } else if ("hprof".equals(profiler)) {
            args.add("java");
            args.add(settings.hprofSettings);
        } else if ("flightrecorder".equals(profiler)) {
            args.add("java");
            args.add(settings.flightrecorderSettings);
        } else {
            args.add("java");
        }

        args.add("-XX:OnOutOfMemoryError=\"touch worker.oome\"");
        args.add("-DSIMULATOR_HOME=" + SIMULATOR_HOME);
        args.add("-Dhazelcast.logging.type=log4j");
        args.add("-DworkerId=" + workerJvm.id);
        args.add("-DworkerMode=" + mode);
        args.add("-DautoCreateHZInstances=" + settings.autoCreateHZInstances);
        args.add("-Dlog4j.configuration=file:" + log4jFile.getAbsolutePath());
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
                    String message = format("Startup failure: worker on host %s failed during startup, "
                                    + "check '%s/out.log' for more info",
                            getHostAddress(), jvm.workerHome
                    );
                    throw new SpawnWorkerFailedException(message);
                }

                String address = readAddress(jvm);

                if (address != null) {
                    jvm.memberAddress = address;

                    it.remove();
                    LOGGER.info(format("Worker: %s Started %s of %s",
                            jvm.id, workers.size() - todo.size(), workers.size()));
                }
            }

            if (todo.isEmpty()) {
                return;
            }

            sleepSeconds(1);
        }

        workerTimeout(workerTimeoutSec, todo);
    }

    private void workerTimeout(int workerTimeoutSec, List<WorkerJvm> todo) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        sb.append(todo.get(0).id);
        for (int l = 1; l < todo.size(); l++) {
            sb.append(",").append(todo.get(l).id);
        }
        sb.append("]");

        throw new SpawnWorkerFailedException(format(
                "Timeout: workers %s of testsuite %s on host %s didn't start within %s seconds",
                sb, agent.getTestSuite().id, getHostAddress(),
                workerTimeoutSec));
    }

    private String readAddress(WorkerJvm jvm) {
        File file = new File(jvm.workerHome, "worker.address");
        if (!file.exists()) {
            return null;
        }

        String address = readObject(file);
        file.delete();
        return address;
    }
}
