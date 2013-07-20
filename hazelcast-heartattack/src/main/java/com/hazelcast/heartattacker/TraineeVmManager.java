package com.hazelcast.heartattacker;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import static com.hazelcast.heartattacker.Utils.getHeartAttackHome;
import static java.lang.String.format;

public class TraineeVmManager {

    final static ILogger log = Logger.getLogger(TraineeVmManager.class);
    private final AtomicBoolean javaHomePrinted = new AtomicBoolean();
    public final static File userDir = new File(System.getProperty("user.dir"));
    public final static String classpath = System.getProperty("java.class.path");
    public final static File heartAttackHome = getHeartAttackHome();
    public final static String classpathSperator = System.getProperty("path.separator");
    public final static AtomicLong TRAINEE_ID_GENERATOR = new AtomicLong();


    private final List<TraineeVm> traineeJvms = new CopyOnWriteArrayList<TraineeVm>();
    private final Coach coach;
    private HazelcastInstance traineeClient;
    private IExecutorService traineeExecutor;

    public TraineeVmManager(Coach coach) {
        this.coach = coach;

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                for (TraineeVm jvm : traineeJvms) {
                    log.log(Level.INFO, "Destroying trainee : " + jvm.getId());
                    jvm.getProcess().destroy();
                }
            }
        });
    }

    public List<TraineeVm> getTraineeJvms() {
        return traineeJvms;
    }

    public void spawn(TraineeVmSettings settings) throws Exception {
        log.log(Level.INFO, format("Starting %s trainee Java Virtual Machines using settings\n %s", settings.getTraineeCount(), settings));

        File traineeHzFile = File.createTempFile("trainee-hazelcast", "xml");
        traineeHzFile.deleteOnExit();
        Utils.writeText(settings.getHzConfig(), traineeHzFile);

        List<TraineeVm> trainees = new LinkedList<TraineeVm>();

        for (int k = 0; k < settings.getTraineeCount(); k++) {
            TraineeVm trainee = startTraineeJvm(settings, traineeHzFile);
            Process process = trainee.getProcess();
            String traineeId = trainee.getId();

            trainees.add(trainee);

            new TraineeVmLogger(traineeId, process.getInputStream(), settings.isTrackLogging()).start();
        }
        Config config = new XmlConfigBuilder(traineeHzFile.getAbsolutePath()).build();
        ClientConfig clientConfig = new ClientConfig().addAddress("localhost:" + config.getNetworkConfig().getPort());
        clientConfig.getGroupConfig()
                .setName(config.getGroupConfig().getName())
                .setPassword(config.getGroupConfig().getPassword());
        traineeClient = HazelcastClient.newHazelcastClient(clientConfig);
        traineeExecutor = traineeClient.getExecutorService(Trainee.TRAINEE_EXECUTOR);

        waitForTraineesStartup(trainees, settings.getTraineeStartupTimeout());

        log.log(Level.INFO, format("Finished starting %s trainee Java Virtual Machines", settings.getTraineeCount()));
    }

    private String getJavaHome(String javaVendor, String javaVersion) {
        JavaInstallation installation = coach.getJavaInstallationRepository().get(javaVendor,javaVersion);
        if(installation!=null){
            //todo: we should send a signal
            return installation.getJavaHome();
        }

        //nothing is found so we are going to make use of a default.
        String javaHome = System.getProperty("java.home");
        if (javaHomePrinted.compareAndSet(false, true)) {
            log.log(Level.INFO, "java.home=" + javaHome);
        }

        return javaHome;
    }

    private TraineeVm startTraineeJvm(TraineeVmSettings settings, File traineeHzFile) throws IOException {
        String traineeId = "trainee-" + TRAINEE_ID_GENERATOR.incrementAndGet();

        String traineeVmOptions = settings.getVmOptions();
        String[] clientVmOptionsArray = new String[]{};
        if (traineeVmOptions != null && !traineeVmOptions.trim().isEmpty()) {
            clientVmOptionsArray = traineeVmOptions.split("\\s+");
        }

        File workoutHome = coach.getWorkoutHome();
        String javaHome = getJavaHome(settings.getJavaVendor(),settings.getJavaVersion());

        List<String> args = new LinkedList<String>();
        args.add("java");
        args.add(format("-XX:OnOutOfMemoryError=\"\"touch %s.oome\"\"", traineeId));
        args.add("-DHEART_ATTACK_HOME=" + getHeartAttackHome());
        args.add("-Dhazelcast.logging.type=log4j");
        args.add("-DtraineeId=" + traineeId);
        args.add("-Dlog4j.configuration=file:" + heartAttackHome + File.separator + "conf" + File.separator + "trainee-log4j.xml");
        args.add("-classpath");

        File libDir = new File(coach.getWorkoutHome(), "lib");
        String s = classpath + classpathSperator + new File(libDir, "*").getAbsolutePath();
        args.add(s);

        args.addAll(Arrays.asList(clientVmOptionsArray));
        args.add(Trainee.class.getName());
        args.add(traineeId);
        args.add(traineeHzFile.getAbsolutePath());

        ProcessBuilder processBuilder = new ProcessBuilder(args.toArray(new String[args.size()]))
                .directory(workoutHome)
                .redirectErrorStream(true);

        Map<String, String> environment = processBuilder.environment();
        String path = javaHome+File.pathSeparator+"bin:"+environment.get("PATH");
        environment.put("PATH",path);
        environment.put("JAVA_HOME", javaHome);

        Process process = processBuilder.start();
        final TraineeVm traineeJvm = new TraineeVm(traineeId, process);
        traineeJvms.add(traineeJvm);
        return traineeJvm;
    }

    private void waitForTraineesStartup(List<TraineeVm> trainees, int traineeTimeoutSec) throws InterruptedException {
        List<TraineeVm> todo = new ArrayList<TraineeVm>(trainees);

        for (int l = 0; l < traineeTimeoutSec; l++) {
            for (Iterator<TraineeVm> it = todo.iterator(); it.hasNext(); ) {
                TraineeVm jvm = it.next();

                InetSocketAddress address = readAddress(jvm);

                if (address != null) {
                    Member member = null;
                    for (Member m : traineeClient.getCluster().getMembers()) {
                        if (m.getInetSocketAddress().equals(address)) {
                            member = m;
                            break;
                        }
                    }

                    if (member != null) {
                        it.remove();
                        jvm.setMember(member);
                        log.log(Level.INFO, format("Trainee: %s Started %s of %s", jvm.getId(), trainees.size() - todo.size(), trainees.size()));
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
            sb.append(",").append(todo.get(l));
        }
        sb.append("]");

        throw new RuntimeException(format("Timeout: trainees %s of workout %s on host %s didn't start within %s seconds",
                sb, coach.getWorkout().getId(), coach.getCoachHz().getCluster().getLocalMember().getInetSocketAddress(), traineeTimeoutSec));
    }

    private InetSocketAddress readAddress(TraineeVm jvm) {
        File workoutHome = coach.getWorkoutHome();

        File file = new File(workoutHome, jvm.getId() + ".address");
        if (!file.exists()) {
            return null;
        }

        return (InetSocketAddress) Utils.readObject(file);
    }

    public void destroyAll() {
        if (traineeClient != null) {
            traineeClient.getLifecycleService().shutdown();
        }

        List<TraineeVm> trainees = new LinkedList<TraineeVm>(traineeJvms);
        traineeJvms.clear();

        for (TraineeVm jvm : trainees) {
            jvm.getProcess().destroy();
        }

        for (TraineeVm jvm : trainees) {
            int exitCode = 0;
            try {
                exitCode = jvm.getProcess().waitFor();
            } catch (InterruptedException e) {
            }

            if (exitCode != 0) {
                log.log(Level.INFO, format("trainee process %s exited with exit code: %s", jvm.getId(), exitCode));
            }
        }
    }

    public IExecutorService getTraineeExecutor() {
        return traineeExecutor;
    }

    public HazelcastInstance getTraineeClient() {
        return traineeClient;
    }

    public void destroy(TraineeVm jvm) {
        jvm.getProcess().destroy();
        try {
            jvm.getProcess().waitFor();
        } catch (InterruptedException e) {
        }
        traineeJvms.remove(jvm);
    }

    public TraineeVm getTrainee(String traineeId) {
        for(TraineeVm trainee: traineeJvms){
            if(traineeId.equals(trainee.getId())){
                return trainee;
            }
        }
        return null;
    }
}
