package com.hazelcast.heartattacker;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.InetSocketAddress;
import java.util.logging.Level;

import static com.hazelcast.heartattacker.Utils.writeObject;
import static java.lang.String.format;

public class Trainee {

    final static ILogger log = Logger.getLogger(Trainee.class.getName());

    public static final String TRAINEE_EXECUTOR = "Trainee:Executor";

    private String traineeId;
    private HazelcastInstance hz;
    private String traineeHzFile;

    public void setTraineeId(String traineeId) {
        this.traineeId = traineeId;
    }

    public void setTraineeHzFile(String traineeHzFile) {
        this.traineeHzFile = traineeHzFile;
    }

    public void start() {
        log.log(Level.INFO, "Creating Trainee HazelcastInstance");
        this.hz = createHazelcastInstance();
        log.log(Level.INFO, "Successfully created Trainee HazelcastInstance");

        signalStartToCoach();
    }

    private void signalStartToCoach() {
        InetSocketAddress address = hz.getCluster().getLocalMember().getInetSocketAddress();
        File file = new File(traineeId + ".address");
        writeObject(address, file);
    }

    private HazelcastInstance createHazelcastInstance() {
        XmlConfigBuilder configBuilder;
        try {
            configBuilder = new XmlConfigBuilder(traineeHzFile);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        Config config = configBuilder.build();
        return Hazelcast.newHazelcastInstance(config);
    }

    private static void logInterestingSystemProperties() {
        logSystemProperty("java.class.path");
        logSystemProperty("java.home");
        logSystemProperty("java.vendor");
        logSystemProperty("java.vendor.url");
        logSystemProperty("java.version");
        logSystemProperty("os.arch");
        logSystemProperty("os.name");
        logSystemProperty("os.version");
        logSystemProperty("user.dir");
        logSystemProperty("user.home");
        logSystemProperty("user.name");
        logSystemProperty("HEART_ATTACK_HOME");
        logSystemProperty("hazelcast.logging.type");
        logSystemProperty("traineeId");
        logSystemProperty("log4j.configuration");
    }

    private static void logSystemProperty(String name) {
        log.log(Level.INFO, format("%s=%s", name, System.getProperty(name)));
    }

    public static void main(String[] args) {
        log.log(Level.INFO, "Starting Hazelcast Heart Attack Trainee");
        logInterestingSystemProperties();

        String traineeId = args[0];
        log.log(Level.INFO, "Trainee id:" + traineeId);
        String traineeHzFile = args[1];
        log.log(Level.INFO, "Trainee hz config file:" + traineeHzFile);

        System.setProperty("traineeId", traineeId);

        Trainee trainee = new Trainee();
        trainee.setTraineeId(traineeId);
        trainee.setTraineeHzFile(traineeHzFile);
        trainee.start();

        log.log(Level.INFO, "Successfully started Hazelcast Heart Attack Trainee:" + traineeId);
    }
}
