package com.hazelcast.stabilizer.common.messaging;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Member;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public abstract class RunOnOldestWorkerMessage extends RunnableMessage implements HazelcastInstanceAware {
    private static final Logger log = Logger.getLogger(RunOnOldestWorkerMessage.class);
    private static final long DELAY = 10; //seconds

    private HazelcastInstance hazelcastInstance;

    public RunOnOldestWorkerMessage(MessageAddress messageAddress) {
        super(messageAddress);
    }

    protected abstract void onOldest();

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public void run() {
        if (isOldest()) {
            scheduleAnotherMasterCheck();
        }
    }

    private void scheduleAnotherMasterCheck() {
        Executors.newSingleThreadScheduledExecutor().schedule(new Runnable() {
            @Override
            public void run() {
                if (isOldest()) {
                    onOldest();
                }
            }
        }, DELAY, TimeUnit.SECONDS);
    }

    private boolean isOldest() {
        Iterator<Member> memberIterator = hazelcastInstance.getCluster().getMembers().iterator();
        boolean master = memberIterator.hasNext() && memberIterator.next().equals(hazelcastInstance.getLocalEndpoint());
        if (log.isDebugEnabled()) {
            log.debug("Am I the oldest member in a cluster? "+master);
        }
        return master;
    }


}
