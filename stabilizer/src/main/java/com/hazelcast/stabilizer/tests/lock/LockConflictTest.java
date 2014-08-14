package com.hazelcast.stabilizer.tests.lock;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.helpers.TxnCounter;
import com.hazelcast.stabilizer.tests.map.helpers.KeyInc;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;
import com.hazelcast.transaction.TransactionContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/*
*
* */
public class LockConflictTest {

    public String basename = this.getClass().getName();
    public int threadCount = 3;
    public int keyCount = 50;
    public int maxKeysPerTxn =5;

    private HazelcastInstance targetInstance;
    private TestContext testContext;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
    }

    @Warmup(global = true)
    public void warmup() throws Exception {
        IList<Long> acounts = targetInstance.getList(basename);
        for (int k = 0; k < keyCount; k++) {
            acounts.add(0l);
        }
    }

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
        private final long[] localIncrements = new long[keyCount];

        @Override
        public void run() {
            while (!testContext.isStopped()) {

                List<KeyInc> potentialLocks = new ArrayList();
                for(int i=0; i< maxKeysPerTxn; i++){
                    KeyInc p = new KeyInc();
                    p.key = random.nextInt(keyCount);
                    p.inc = random.nextInt(999);
                    potentialLocks.add(p);
                }

                List<KeyInc> locked = new ArrayList();
                for(KeyInc i : potentialLocks){
                    try{
                        ILock lock = targetInstance.getLock(basename + "l" + i.key);
                        try{
                            if( lock.tryLock(10, TimeUnit.MILLISECONDS) ){
                                locked.add(i);
                            }
                        }catch(Exception e){
                            System.out.println(basename+": trying lock="+i.key+" "+e);
                        }
                    }catch(Exception e){
                        System.out.println(basename+": getting lock for locking="+i.key+" "+e);
                    }
                }

                for(KeyInc i : locked){
                    try {
                        IList<Long> acounts = targetInstance.getList(basename);
                        long value = acounts.get(i.key);
                        acounts.set(i.key, value + i.inc);
                        localIncrements[i.key]+=i.inc;
                    }catch(Exception e){
                        System.out.println(basename+": updating acount="+i+" "+e);
                    }
                }

                int unlockAttempts=0;
                while(!locked.isEmpty()){
                    Iterator<KeyInc> ittr = locked.iterator();
                    while(ittr.hasNext()){
                        KeyInc i = ittr.next();
                        try{
                            ILock lock = targetInstance.getLock(basename + "l" + i.key);
                            try{
                                lock.unlock();
                                ittr.remove();
                            }catch(Exception e){
                                System.out.println(basename+": unlocking lock ="+i.key+" "+e);
                            }
                        }catch(Exception e){
                            System.out.println(basename+": getting lock for unlocking="+i.key+" "+e);
                        }
                    }

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e){}

                    if(++unlockAttempts > 5){
                        System.out.println(basename+": Cant unlonck="+locked+" unlockAttempts="+unlockAttempts);
                        break;
                    }
                }

            }
            targetInstance.getList(basename+"res").add(localIncrements);
        }
    }

    @Verify(global = false)
    public void verify() throws Exception {

        IList<long[]> allIncrements = targetInstance.getList(basename+"res");
        long expected[] = new long[keyCount];
        for (long[] incs : allIncrements) {
            for (int i=0; i < incs.length; i++) {
                expected[i] += incs[i];
            }
        }
        System.out.println(basename+": results from "+allIncrements.size()+" worker Threads");

        IList<Long> acounts = targetInstance.getList(basename);
        int failures = 0;
        for (int k = 0; k < keyCount; k++) {
            if (expected[k] != acounts.get(k)) {
                failures++;

                System.out.println(basename + ": key=" + k + " expected " + expected[k] + " != " + "actual " + acounts.get(k));
            }
        }

        assertEquals(basename+": "+failures+" key=>values have been incremented unExpected", 0, failures);
    }

}