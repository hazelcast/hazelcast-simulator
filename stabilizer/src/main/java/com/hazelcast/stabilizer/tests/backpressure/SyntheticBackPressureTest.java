package com.hazelcast.stabilizer.tests.backpressure;

import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.proxy.PartitionServiceProxy;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.OperationService;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.TestRunner;
import com.hazelcast.stabilizer.tests.annotations.Performance;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Teardown;
import com.hazelcast.stabilizer.tests.utils.ExceptionReporter;
import com.hazelcast.stabilizer.tests.utils.PropertyBindingSupport;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.stabilizer.tests.utils.TestUtils.getNode;
import static com.hazelcast.stabilizer.tests.utils.TestUtils.getOperationCountInformation;
import static com.hazelcast.stabilizer.tests.utils.TestUtils.getPartitionDistributionInformation;
import static com.hazelcast.stabilizer.tests.utils.TestUtils.isClient;


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
    public int syncFrequency = 1;

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
        log.info(getPartitionDistributionInformation(targetInstance));
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
        private final ArrayList<Integer> partitionSequence = new ArrayList<Integer>();
        private final boolean isClient;
        private final ClientInvocationService clientInvocationService;
        private final ClientPartitionService clientPartitionService;
        private final ArrayList<ICompletableFuture> futureList = new ArrayList<ICompletableFuture>(syncFrequency);
        int partitionIndex = 0;

        public Worker() {
            isClient = isClient(targetInstance);
            if (isClient) {
                HazelcastClientProxy hazelcastClientProxy = (HazelcastClientProxy) targetInstance;
                operationService = null;
                PartitionServiceProxy partitionService = (PartitionServiceProxy) hazelcastClientProxy.client.getPartitionService();
                clientPartitionService = PropertyBindingSupport.getField(partitionService, "partitionService");
                clientInvocationService = hazelcastClientProxy.client.getInvocationService();
                partitionCount = clientPartitionService.getPartitionCount();
            } else {
                clientInvocationService = null;
                clientPartitionService = null;
                Node node = getNode(context.getTargetInstance());
                node.getPartitionService().getPartitionCount();
                operationService = node.getNodeEngine().getOperationService();
                partitionCount = node.getPartitionService().getPartitionCount();
            }

            if (randomPartition) {
                for (int k = 0; k < 10; k++) {
                    for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                        partitionSequence.add(partitionId);
                    }
                }
                Collections.shuffle(partitionSequence);
            }
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
            try {
                doRun();
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public void doRun() throws Exception {
            long iteration = 0;

            while (!context.isStopped()) {

                int partitionId = nextPartitionId();

                ICompletableFuture f = invoke(partitionId);
                if (syncInvocation) {
                    if (syncFrequency == 1) {
                        f.get();
                    } else {
                        if (iteration > 0 && iteration % syncFrequency == 0) {
                            for (ICompletableFuture future : futureList) {
                                future.get();
                            }
                            futureList.clear();
                        } else {
                            futureList.add(f);
                        }
                    }
                } else {
                    f.andThen(this);
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


        private ICompletableFuture invoke(int partitionId) throws Exception {
            ICompletableFuture f;
            if (isClient) {
                SomeRequest request = new SomeRequest(syncBackupCount, asyncBackupCount, backupDelayNanos);
                request.setPartitionId(partitionId);
                Address target = clientPartitionService.getPartitionOwner(partitionId);
                f = clientInvocationService.invokeOnTarget(request, target);
            } else {
                SomeOperation operation = new SomeOperation(syncBackupCount, asyncBackupCount, getBackupDelayNanos());
                f = operationService.invokeOnPartition(null, operation, partitionId);
            }
            return f;
        }

        private int nextPartitionId() {
            int partitionId;
            if (randomPartition) {
                partitionId = partitionSequence.get(partitionIndex);
                partitionIndex++;
                if (partitionIndex >= partitionSequence.size()) {
                    partitionIndex = 0;
                }
            } else {
                partitionId = 0;
            }
            return partitionId;
        }

        private long getBackupDelayNanos() {
            if (!randomizeBackupDelay) {
                return backupDelayNanos;
            }

            if (backupDelayNanos == 0) {
                return 0;
            }

            long d = Math.abs(random.nextLong());
            return d % backupDelayNanos;
        }
    }

    public static void main(String[] args) throws Throwable {
        SyntheticBackPressureTest test = new SyntheticBackPressureTest();
        new TestRunner(test).withDuration(10).run();
    }
}
