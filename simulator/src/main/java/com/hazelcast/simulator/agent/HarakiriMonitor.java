package com.hazelcast.simulator.agent;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.provisioner.Bash;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

/**
 * Responsible for terminating ec2-instances if they are not used to prevent running into a big bill.
 */
public class HarakiriMonitor extends Thread {
    private static final Logger LOGGER = Logger.getLogger(HarakiriMonitor.class);
    private final Agent agent;

    public HarakiriMonitor(Agent agent) {
        super("HarakiriMonitor");
        this.agent = agent;
    }

    public void run() {
        if (!"aws-ec2".equals(agent.cloudProvider)) {
            LOGGER.info("No Harakiri monitor is active: only on aws-ec2 unused machines will be terminated.");
            return;
        }

        LOGGER.info("Harakiri monitor is active");

        for (; ; ) {
            try {
                Thread.sleep(TimeUnit.MINUTES.toMillis(1));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            long maxIdleTimeMs = TimeUnit.HOURS.toMillis(2);

            boolean harakiri = System.currentTimeMillis() - maxIdleTimeMs > agent.lastUsed;
            if (harakiri) {
                LOGGER.info("Trying to commit Harakiri (will only try once)");
                Bash bash = new Bash(new SimulatorProperties());
                try {
                    String cmd = format("ec2-terminate-instances $(curl -s http://169.254.169.254/latest/meta-data/instance-id) "
                            + "--aws-access-key %s --aws-secret-key %s", agent.cloudIdentity, agent.cloudCredential);
                    LOGGER.info("harakiri command: " + cmd);
                    bash.execute(cmd);
                } catch (RuntimeException e) {
                    LOGGER.info("Failed to execute Harakiri");
                }
                return;
            }
        }
    }
}
