package com.hazelcast.simulator.tests.concurrent.lock;

import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;

import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.fail;

public class LeaseLockTest extends HazelcastTest {

   public int lockCount = 500;
   public int maxLeaseTimeMillis = 100;
   public int maxTryTimeMillis = 100;

   @TimeStep
   public void timeStep(BaseThreadState state) {
      int lockIndex = state.randomInt(lockCount);
      FencedLock lock = targetInstance.getCPSubsystem().getLock(name + lockIndex);

      int tryTime = 1 + state.randomInt(maxTryTimeMillis);

      lock.tryLock(tryTime, MILLISECONDS);
      if (lock.isLockedByCurrentThread()) {
         lock.unlock();
      }
   }

   @AfterRun
   public void afterRun() throws Exception {
      sleepMillis((maxTryTimeMillis + maxLeaseTimeMillis) * 2);
   }

   @Verify
   public void verify() {
      for (int i = 0; i < lockCount; i++) {
         FencedLock lock = targetInstance.getCPSubsystem().getLock(name + i);
         if (lock.isLocked()) {
            fail("All of the locks should unlocked in the end. Lock " + i + " is still locked.");
         }
      }
   }
}