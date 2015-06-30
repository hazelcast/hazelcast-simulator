package com.hazelcast.simulator.agent;

import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.CloudProviderUtils.isEC2;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.NativeUtils.execute;
import static java.lang.String.format;

/**
 * Responsible for terminating ec2-instances if they are not used to prevent running into a big bill.
 */
public class HarakiriMonitor extends Thread {

    private static final long MAX_IDLE_TIME_MS = TimeUnit.HOURS.toMillis(2);

    private static final Logger LOGGER = Logger.getLogger(HarakiriMonitor.class);

    private final Agent agent;

    public HarakiriMonitor(Agent agent) {
        super("HarakiriMonitor");
        this.agent = agent;
    }

    public void run() {
        if (!isEC2(agent.cloudProvider)) {
            LOGGER.info("No Harakiri monitor is active: only on AWS-EC2 unused machines will be terminated.");
            return;
        }

        LOGGER.info("Harakiri monitor is active");
        for (; ; ) {
            sleepSeconds((int) TimeUnit.MINUTES.toSeconds(1));

            boolean doHarakiri = (System.currentTimeMillis() - MAX_IDLE_TIME_MS > agent.lastUsed);
            if (doHarakiri) {
                LOGGER.info("Trying to commit Harakiri once!");
                try {
                    String cmd = format("ec2-terminate-instances $(curl -s http://169.254.169.254/latest/meta-data/instance-id) "
                            + "--aws-access-key %s --aws-secret-key %s", agent.cloudIdentity, agent.cloudCredential);
                    LOGGER.info("harakiri command: " + cmd);
                    execute(cmd);
                } catch (RuntimeException e) {
                    LOGGER.info("Failed to execute Harakiri");
                }
                return;
            }
        }
    }
}
