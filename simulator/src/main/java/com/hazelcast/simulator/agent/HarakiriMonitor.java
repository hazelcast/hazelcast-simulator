package com.hazelcast.simulator.agent;

import com.hazelcast.simulator.utils.CommandLineExitException;
import org.apache.log4j.Logger;

import static com.hazelcast.simulator.utils.CloudProviderUtils.isEC2;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.NativeUtils.execute;
import static java.lang.String.format;

/**
 * Responsible for terminating EC2 instances if they are not used to prevent running into a high bill.
 */
public class HarakiriMonitor {

    private static final Logger LOGGER = Logger.getLogger(HarakiriMonitor.class);

    private final String cloudProvider;
    private final String cloudIdentity;
    private final String cloudCredential;
    private final int waitSeconds;

    public HarakiriMonitor(String cloudProvider, String cloudIdentity, String cloudCredential, int waitSeconds) {
        this.cloudProvider = cloudProvider;
        this.cloudIdentity = cloudIdentity;
        this.cloudCredential = cloudCredential;
        this.waitSeconds = waitSeconds;
    }

    void start() {
        if (!isEC2(cloudProvider)) {
            LOGGER.info("No Harakiri monitor is active: only on AWS-EC2 unused machines will be terminated.");
            return;
        }

        LOGGER.info(format("Harakiri monitor is active and will wait %d seconds to kill this instance", waitSeconds));
        sleepSeconds(waitSeconds);

        LOGGER.info("Trying to commit Harakiri once!");
        try {
            String cmd = format("ec2-terminate-instances $(curl -s http://169.254.169.254/latest/meta-data/instance-id) "
                    + "--aws-access-key %s --aws-secret-key %s", cloudIdentity, cloudCredential);
            LOGGER.info("Harakiri command: " + cmd);
            execute(cmd);
        } catch (Exception e) {
            throw new CommandLineExitException("Failed to execute Harakiri", e);
        }
    }

    public static void main(String[] args) {
        try {
            HarakiriMonitor harakiriMonitor = HarakiriMonitorCli.createHarakiriMonitor(args);
            harakiriMonitor.start();
        } catch (Exception e) {
            exitWithError(LOGGER, "Could not start HarakiriMonitor!", e);
        }
    }
}
