package com.hazelcast.stabilizer.tests.synthetic;

import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.proxy.PartitionServiceProxy;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Partition;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.OperationService;
import com.hazelcast.stabilizer.test.TestContext;
import com.hazelcast.stabilizer.test.TestRunner;
import com.hazelcast.stabilizer.test.annotations.Performance;
import com.hazelcast.stabilizer.test.annotations.Run;
import com.hazelcast.stabilizer.test.annotations.Setup;
import com.hazelcast.stabilizer.test.annotations.Teardown;
import com.hazelcast.stabilizer.test.utils.ThreadSpawner;
import com.hazelcast.stabilizer.tests.helpers.KeyLocality;
import com.hazelcast.stabilizer.tests.helpers.KeyUtils;
import com.hazelcast.stabilizer.utils.ExceptionReporter;
import com.hazelcast.stabilizer.utils.ReflectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.stabilizer.tests.helpers.HazelcastTestUtils.getNode;
import static com.hazelcast.stabilizer.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.stabilizer.tests.helpers.HazelcastTestUtils.getPartitionDistributionInformation;
import static com.hazelcast.stabilizer.tests.helpers.HazelcastTestUtils.isClient;

/**
 * The SyntheticTest can be used to test features like back pressure.
 * <p/>
 * It can be configured with:
 * - sync invocation
 * - async invocation
 * - number of sync backups
 * - number of async backups
 * - delay of back pressing.
 * <p/>
 * This test doesn't make use of any normal data-structures like an {@link com.hazelcast.core.IMap}, but uses the SPI directly to
 * execute operations and backups. This gives a lot of control on the behavior.
 * <p/>
 * If for example we want to test back pressure on async backups, just set the asyncBackupCount to a value larger than 0 and if
 * you want to simulate a slow down, also set the backupDelayNanos. If this is set to a high value, on the backup you will get a
 * pileup of back up commands which eventually can lead to an OOME.
 * <p/>
 * Another interesting scenario to test is a normal async invocation of a readonly operation (so no async/sync-backups) and see if
 * the system can be flooded with too many request. Normal sync operations don't cause that many problems because there is a
 * natural balance between the number of threads and the number of pending invocations.
 */
public class SyntheticTest {
    private static final ILogger log = Logger.getLogger(SyntheticTest.class);

    // properties
    public boolean syncInvocation = true;
    public byte syncBackupCount = 0;
    public byte asyncBackupCount = 1;
    public long backupDelayNanos = 1000 * 1000;
    public boolean randomizeBackupDelay = true;
    public int threadCount = 10;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 1000;
    public KeyLocality keyLocality = KeyLocality.Random;
    public int keyCount = 1000;
    public int syncFrequency = 1;
    public String serviceName;

    private final AtomicLong operations = new AtomicLong();

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    //private IntervalProbe probe;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
    }

    @Teardown
    public void teardown() throws Exception {
        log.info(getOperationCountInformation(targetInstance));
        log.info(getPartitionDistributionInformation(targetInstance));
    }

    @Performance
    public long getOperationCount() {
        return operations.get();
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int i = 0; i < threadCount; i++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    private class Worker implements Runnable, ExecutionCallback<Object> {
        private final ArrayList<Integer> partitionSequence = new ArrayList<Integer>();
        private final ArrayList<ICompletableFuture> futureList = new ArrayList<ICompletableFuture>(syncFrequency);
        private final Random random = new Random();

        private final boolean isClient;
        private final OperationService operationService;
        private final ClientInvocationService clientInvocationService;
        private final ClientPartitionService clientPartitionService;

        private int partitionIndex = 0;

        public Worker() {
            isClient = isClient(targetInstance);

            if (isClient) {
                HazelcastClientProxy hazelcastClientProxy = (HazelcastClientProxy) targetInstance;
                PartitionServiceProxy partitionService = (PartitionServiceProxy) hazelcastClientProxy.client.getPartitionService();

                operationService = null;
                clientInvocationService = hazelcastClientProxy.client.getInvocationService();
                clientPartitionService = ReflectionUtils.getObjectFromField(partitionService, "partitionService");
            } else {
                Node node = getNode(targetInstance);

                operationService = node.getNodeEngine().getOperationService();
                clientInvocationService = null;
                clientPartitionService = null;
            }

            if (isClient && keyLocality == KeyLocality.Local) {
                throw new IllegalStateException("A KeyLocality has been set to Local, but test is running on a client."
                        + " It doesn't make sense as no keys are stored on clients. ");
            }

            for (int i = 0; i < keyCount; i++) {
                Integer key = KeyUtils.generateIntKey(keyCount, keyLocality, targetInstance);
                Partition partition = targetInstance.getPartitionService().getPartition(key);
                partitionSequence.add(partition.getPartitionId());
            }
            Collections.shuffle(partitionSequence);
        }

        @Override
        public void onResponse(Object response) {
            operations.addAndGet(1);
        }

        @Override
        public void onFailure(Throwable t) {
            ExceptionReporter.report(testContext.getTestId(), t);
        }

        @Override
        public void run() {
            try {
                doRun();
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private void doRun() throws Exception {
            long iteration = 0;

            while (!testContext.isStopped()) {
                int partitionId = nextPartitionId();
                //probe.started();

                ICompletableFuture<Object> future = invoke(partitionId);

                if (syncInvocation) {
                    if (syncFrequency == 1) {
                        future.get();
                    } else {
                        if (iteration > 0 && iteration % syncFrequency == 0) {
                            for (ICompletableFuture innerFuture : futureList) {
                                innerFuture.get();
                            }
                            futureList.clear();
                        } else {
                            futureList.add(future);
                        }
                    }
                } else {
                    future.andThen(this);
                }
                //probe.done();

                iteration++;
                if (iteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " at iteration: " + iteration);
                }

                if (iteration % performanceUpdateFrequency == 0) {
                    if (syncInvocation) {
                        operations.addAndGet(performanceUpdateFrequency);
                    }
                }
            }
            operations.addAndGet(iteration % performanceUpdateFrequency);
        }

        private ICompletableFuture<Object> invoke(int partitionId) throws Exception {
            ICompletableFuture<Object> future;
            if (isClient) {
                SyntheticRequest request = new SyntheticRequest(syncBackupCount, asyncBackupCount, backupDelayNanos, null);
                request.setPartitionId(partitionId);
                Address target = clientPartitionService.getPartitionOwner(partitionId);
                future = clientInvocationService.invokeOnTarget(request, target);
            } else {
                SyntheticOperation operation = new SyntheticOperation(syncBackupCount, asyncBackupCount, getBackupDelayNanos());
                future = operationService.invokeOnPartition(serviceName, operation, partitionId);
            }
            return future;
        }

        private int nextPartitionId() {
            int partitionId = partitionSequence.get(partitionIndex);
            partitionIndex++;
            if (partitionIndex >= partitionSequence.size()) {
                partitionIndex = 0;
            }
            return partitionId;
        }

        private long getBackupDelayNanos() {
            if (syncBackupCount == 0 && asyncBackupCount == 0) {
                return 0;
            }

            if (!randomizeBackupDelay) {
                return backupDelayNanos;
            }

            if (backupDelayNanos == 0) {
                return 0;
            }

            return Math.abs(random.nextLong() + 1) % backupDelayNanos;
        }
    }

    public static void main(String[] args) throws Throwable {
        SyntheticTest test = new SyntheticTest();
        new TestRunner<SyntheticTest>(test).withDuration(10).run();
    }
}
