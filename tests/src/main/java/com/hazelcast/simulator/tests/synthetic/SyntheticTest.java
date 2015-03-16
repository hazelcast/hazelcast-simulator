package com.hazelcast.simulator.tests.synthetic;

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
import com.hazelcast.simulator.probes.probes.IntervalProbe;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.Performance;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.tests.helpers.KeyUtils;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.test.utils.ThreadSpawner;
import com.hazelcast.simulator.utils.ReflectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getNode;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getPartitionDistributionInformation;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isClient;

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
public class SyntheticTest {
    private static final ILogger log = Logger.getLogger(SyntheticTest.class);

    //props
    public boolean syncInvocation = true;
    public byte syncBackupCount = 0;
    public byte asyncBackupCount = 1;
    public long backupDelayNanos = 1000 * 1000;
    public boolean randomizeBackupDelay = true;
    public int threadCount = 10;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 1000;
    public KeyLocality keyLocality = KeyLocality.Random;
    public int syncFrequency = 1;
    public String serviceName;

    private AtomicLong operations = new AtomicLong();
    private TestContext context;
    private HazelcastInstance targetInstance;
    public IntervalProbe latency;

    @Setup
    public void setup(TestContext context) throws Exception {
        this.context = context;
        this.targetInstance = context.getTargetInstance();
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
        private final ArrayList<Integer> partitionSequence = new ArrayList<Integer>();
        private final Random random = new Random();
        private final OperationService operationService;
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
                clientPartitionService = ReflectionUtils.getObjectFromField(partitionService, "partitionService");
                clientInvocationService = hazelcastClientProxy.client.getInvocationService();
            } else {
                clientInvocationService = null;
                clientPartitionService = null;
                Node node = getNode(context.getTargetInstance());
                node.getPartitionService().getPartitionCount();
                operationService = node.getNodeEngine().getOperationService();
            }

            if (isClient) {
                if (keyLocality == KeyLocality.Local)
                    throw new IllegalStateException("A KeyLocality has been set to Local, but test is running on a client. " +
                            "It doesn't make sense as no keys are stored on clients. ");
            }

            int keys = 1000;
            for (int k = 0; k < keys; k++) {
                Integer key = KeyUtils.generateIntKey(keys, keyLocality, targetInstance);
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
                //latency.started();

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
                //latency.done();

                iteration++;
                if (iteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }

                if (iteration % performanceUpdateFrequency == 0) {
                    if (syncInvocation) {
                        operations.addAndGet(performanceUpdateFrequency);
                    }
                }
            }
            operations.addAndGet(iteration % performanceUpdateFrequency);
        }

        private ICompletableFuture invoke(int partitionId) throws Exception {
            ICompletableFuture f;
            if (isClient) {
                SyntheticRequest request = new SyntheticRequest(syncBackupCount, asyncBackupCount, backupDelayNanos, null);
                request.setPartitionId(partitionId);
                Address target = clientPartitionService.getPartitionOwner(partitionId);
                f = clientInvocationService.invokeOnTarget(request, target);
            } else {
                SyntheticOperation operation = new SyntheticOperation(syncBackupCount, asyncBackupCount, getBackupDelayNanos());
                f = operationService.invokeOnPartition(serviceName, operation, partitionId);
            }
            return f;
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
