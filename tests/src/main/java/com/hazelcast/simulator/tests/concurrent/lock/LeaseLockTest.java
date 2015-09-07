package com.hazelcast.simulator.tests.concurrent.lock;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.*;
import com.hazelcast.simulator.utils.ThreadSpawner;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class LeaseLockTest {

    private static final ILogger LOGGER = Logger.getLogger(LeaseLockTest.class);

    public String basename = this.getClass().getSimpleName();
    public int lockCount = 500;
    public int maxleaseTime = 100;
    public int maxTryTime = 100;
    public int threadCount = 3;

    private HazelcastInstance targetInstance;
    private TestContext testContext;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
    }

    @Warmup(global = true)
    public void warmup() throws Exception {}

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    private class Worker implements Runnable {
        private final Random random = new Random();

        public void run() {
            while (!testContext.isStopped()) {
                int i = random.nextInt(lockCount);
                ILock lock = targetInstance.getLock(basename+i);

                int lease = 1 + random.nextInt(maxleaseTime);
                int tryTime = 1 + random.nextInt(maxTryTime);

                if (random.nextBoolean()) {
                    lock.lock(lease, TimeUnit.MILLISECONDS);
                }else {
                    try {
                        lock.tryLock(tryTime, TimeUnit.MILLISECONDS, lease, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        LOGGER.info("try lock throws"+e);
                    }
                }
            }
        }
    }

    @Verify
    public void verify() throws Exception {
        Thread.sleep( (maxTryTime + maxleaseTime) * 2 );

        for(int i=0; i<lockCount; i++){
            ILock lock = targetInstance.getLock(basename+i);

            if (lock.isLocked()){
                throw new Exception(lock+" is locked ! and lease time =" + lock.getRemainingLeaseTime());
            }
            if (lock.getRemainingLeaseTime() > 0){
                throw new Exception(lock+" has lease time =" + lock.getRemainingLeaseTime() +" and is locked =" + lock.isLocked());
            }
        }
    }
}
