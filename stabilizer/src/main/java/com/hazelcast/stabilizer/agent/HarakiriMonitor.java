package com.hazelcast.stabilizer.agent;

import com.hazelcast.stabilizer.common.StabilizerProperties;
import com.hazelcast.stabilizer.provisioner.Bash;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class HarakiriMonitor extends Thread {
    private final static Logger log = Logger.getLogger(HarakiriMonitor.class.getName());
    private final Agent agent;

    public HarakiriMonitor(Agent agent) {
        super("HarakiriMonitor");
        this.agent = agent;
    }

    public void run() {
        if (!"aws-ec2".equals(agent.cloudProvider)) {
            log.info("No Harakiri monitor is active: only on aws-ec2 unused machines will be terminated.");
            return;
        }

        log.info("Harakiri monitor is active");

        for (; ; ) {
            try {
                Thread.sleep(TimeUnit.MINUTES.toMillis(1));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            long maxIdleTimeMs = TimeUnit.HOURS.toMillis(2);

            boolean harakiri = System.currentTimeMillis() - maxIdleTimeMs > agent.lastUsed;
            if (harakiri) {
                log.info("Trying to commit Harakiri (will only try once)");
                Bash bash = new Bash(new StabilizerProperties());
                try {
                    String cmd = format("ec2-terminate-instances $(curl -s http://169.254.169.254/latest/meta-data/instance-id) --aws-access-key %s --aws-secret-key %s", agent.cloudIdentity, agent.cloudCredential);
                    log.info("harakiri command: " + cmd);
                    bash.execute(cmd);
                } catch (RuntimeException e) {
                    log.info("Failed to execute harikiri");
                }
                return;
            }
        }
    }
}
