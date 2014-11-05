package com.hazelcast.stabilizer.tests.backpressure;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.TestRunner;
import com.hazelcast.stabilizer.tests.annotations.Performance;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Teardown;
import com.hazelcast.stabilizer.tests.utils.ExceptionReporter;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.stabilizer.tests.utils.TestUtils.getNode;
import static com.hazelcast.stabilizer.tests.utils.TestUtils.getOperationCountInformation;


/**
 * The SyntheticBackPressureTest tests back pressure.
 * <p/>
 * It can be configured with:
 * - sync invocation
 * - async invocation
 * - number of sync backups
 * - number of async backups
 * - delay of back pressing.
 * <p/>
 * This test doesn't make use of any normal data-structures like an IMap; but uses the SPI directly to execute operations
 * and backups. This gives a lot of control on the behavior.
 * <p/>
 * If for example we want to test back pressure on async backups; just set the asyncBackupCount to a value larger than 0 and
 * if you want to simulate a slow down, also set the backupDelayNanos. If this is set to a high value, on the backup you will
 * get a pileup of back up commands which eventually can lead to an OOME.
 * <p/>
 * Another interesting scenario to test is a normal async invocation of a readonly operation (so no async/sync-backups) and see
 * if the system can be flooded with too many request. Normal sync operations don't cause that many problems because there is a
 * natural balance between the number of threads and the number of pending invocations.
 */
public class SyntheticBackPressureTest {
    private final static ILogger log = Logger.getLogger(SyntheticBackPressureTest.class);

    //props
    public boolean randomPartition = true;
    public boolean syncInvocation = true;
    public int syncBackupCount = 0;
    public int asyncBackupCount = 1;
    public long backupDelayNanos = 1000 * 1000;
    public boolean randomizeBackupDelay = true;
    public int threadCount = 10;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 1000;

    private AtomicLong operations = new AtomicLong();
    private TestContext context;
    private HazelcastInstance targetInstance;

    @Setup
    public void setup(TestContext context) throws Exception {
        this.context = context;

        targetInstance = context.getTargetInstance();
    }

    @Teardown
    public void teardown() throws Exception {
        log.info(getOperationCountInformation(targetInstance));
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(context.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    @Performance
    public long getOperationCount() {
        return operations.get();
    }

    private class Worker implements Runnable, ExecutionCallback {

        private final Random random = new Random();
        private final OperationService operationService;
        private final int partitionCount;

        public Worker() {
            Node node = getNode(context.getTargetInstance());
            node.getPartitionService().getPartitionCount();
            operationService = node.getNodeEngine().getOperationService();
            partitionCount = node.getPartitionService().getPartitionCount();
        }

        @Override
        public void onResponse(Object response) {
            operations.addAndGet(performanceUpdateFrequency);
        }

        @Override
        public void onFailure(Throwable t) {
            ExceptionReporter.report(context.getTestId(), t);
        }

        @Override
        public void run() {
            long iteration = 0;

            while (!context.isStopped()) {
                SomeOperation operation = new SomeOperation(syncBackupCount, asyncBackupCount, getBackupDelayNanos());
                int partitionId = nextPartitionId();
                ICompletableFuture f = operationService.invokeOnPartition("don'tcare", operation, partitionId);
                try {
                    if (syncInvocation) {
                        f.get();
                    } else {
                        f.andThen(this);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }

                if (iteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }

                if (iteration % performanceUpdateFrequency == 0) {
                    if (syncInvocation) {
                        operations.addAndGet(performanceUpdateFrequency);
                    }
                }
                iteration++;
            }
        }

        private long getBackupDelayNanos() {
            if (!randomizeBackupDelay) {
                return backupDelayNanos;
            }

            if(backupDelayNanos == 0){
                return 0;
            }

            long d = Math.abs(random.nextLong());
            return d % backupDelayNanos;
        }

        public int nextPartitionId() {
            if (randomPartition) {
                return random.nextInt(partitionCount);
            } else {
                return 0;
            }
        }
    }


    public static class SomeOperation extends AbstractOperation implements BackupAwareOperation, PartitionAwareOperation {
        private int syncBackupCount;
        private int asyncBackupCount;
        private long backupOperationDelayNanos;

        public SomeOperation() {
        }

        public SomeOperation(int syncBackupCount, int asyncBackupCount, long backupOperationDelayNanos) {
            this.syncBackupCount = syncBackupCount;
            this.asyncBackupCount = asyncBackupCount;
            this.backupOperationDelayNanos = backupOperationDelayNanos;
        }

        @Override
        public boolean shouldBackup() {
            return true;
        }

        @Override
        public int getSyncBackupCount() {
            return syncBackupCount;
        }

        @Override
        public int getAsyncBackupCount() {
            return asyncBackupCount;
        }

        @Override
        public Operation getBackupOperation() {
            SomeBackupOperation someBackupOperation = new SomeBackupOperation(backupOperationDelayNanos);
            someBackupOperation.setPartitionId(getPartitionId());
            return someBackupOperation;
        }

        @Override
        public void run() throws Exception {
            //do nothing
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeInt(syncBackupCount);
            out.writeInt(asyncBackupCount);
            out.writeLong(backupOperationDelayNanos);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);

            syncBackupCount = in.readInt();
            asyncBackupCount = in.readInt();
            backupOperationDelayNanos = in.readLong();
        }
    }

    public static class SomeBackupOperation extends AbstractOperation implements BackupOperation, PartitionAwareOperation {
        private long delayNs;

        public SomeBackupOperation() {
        }

        public SomeBackupOperation(long delayNs) {
            this.delayNs = delayNs;
        }

        @Override
        public void run() throws Exception {
            LockSupport.parkNanos(delayNs);
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeLong(delayNs);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            delayNs = in.readLong();
        }
    }

    public static void main(String[] args) throws Throwable {
        SyntheticBackPressureTest test = new SyntheticBackPressureTest();
        new TestRunner(test).withDuration(10).run();
    }
}
