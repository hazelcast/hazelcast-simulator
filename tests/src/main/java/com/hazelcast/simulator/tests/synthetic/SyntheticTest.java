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
import com.hazelcast.simulator.probes.probes.IntervalProbe;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.Performance;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.tests.helpers.HazelcastTestUtils;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.worker.tasks.IWorker;
import com.hazelcast.spi.OperationService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getNode;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getPartitionDistributionInformation;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isClient;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntKey;
import static com.hazelcast.simulator.utils.ReflectionUtils.getObjectFromField;

/**
 * The SyntheticTest can be used to test features like back pressure.
 *
 * It can be configured with:
 * - sync invocation
 * - async invocation
 * - number of sync backups
 * - number of async backups
 * - delay of back pressing.
 *
 * This test doesn't make use of any normal data-structures like an {@link com.hazelcast.core.IMap}, but uses the SPI directly to
 * execute operations and backups. This gives a lot of control on the behavior.
 *
 * If for example we want to test back pressure on async backups, just set the asyncBackupCount to a value larger than 0 and if
 * you want to simulate a slow down, also set the backupDelayNanos. If this is set to a high value, on the backup you will get a
 * pileup of back up commands which eventually can lead to an OOME.
 *
 * Another interesting scenario to test is a normal async invocation of a readonly operation (so no async/sync-backups) and see if
 * the system can be flooded with too many request. Normal sync operations don't cause that many problems because there is a
 * natural balance between the number of threads and the number of pending invocations.
 */
public class SyntheticTest {

    private static final ILogger LOGGER = Logger.getLogger(SyntheticTest.class);

    // properties
    public boolean syncInvocation = true;
    public byte syncBackupCount = 0;
    public byte asyncBackupCount = 1;
    public long backupDelayNanos = 1000 * 1000;
    public boolean randomizeBackupDelay = true;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 1000;
    public KeyLocality keyLocality = KeyLocality.RANDOM;
    public int keyCount = 1000;
    public int syncFrequency = 1;
    public String serviceName;

    private HazelcastInstance targetInstance;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        targetInstance = testContext.getTargetInstance();
    }

    @Teardown
    public void teardown() throws Exception {
        LOGGER.info(getOperationCountInformation(targetInstance));
        LOGGER.info(getPartitionDistributionInformation(targetInstance));
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker implements IWorker, ExecutionCallback<Object> {

        // these fields will be injected by the TestContainer
        TestContext testContext;
        IntervalProbe intervalProbe;

        private final List<Integer> partitionSequence = new ArrayList<Integer>();
        private final List<ICompletableFuture> futureList = new ArrayList<ICompletableFuture>(syncFrequency);
        private final Random random = new Random();

        private final boolean isClient;
        private final OperationService operationService;
        private final ClientInvocationService clientInvocationService;
        private final ClientPartitionService clientPartitionService;

        private int partitionIndex;
        private long iteration;

        public Worker() {
            isClient = isClient(targetInstance);
            checkClientKeyLocality();

            if (isClient) {
                HazelcastClientProxy hazelcastClientProxy = (HazelcastClientProxy) targetInstance;
                PartitionServiceProxy partitionService
                        = (PartitionServiceProxy) hazelcastClientProxy.client.getPartitionService();

                operationService = null;
                clientInvocationService = hazelcastClientProxy.client.getInvocationService();
                clientPartitionService = getObjectFromField(partitionService, "partitionService");
            } else {
                Node node = getNode(targetInstance);

                operationService = HazelcastTestUtils.getOperationService(targetInstance);
                clientInvocationService = null;
                clientPartitionService = null;
            }

            for (int i = 0; i < keyCount; i++) {
                Integer key = generateIntKey(keyCount, keyLocality, targetInstance);
                Partition partition = targetInstance.getPartitionService().getPartition(key);
                partitionSequence.add(partition.getPartitionId());
            }
            Collections.shuffle(partitionSequence);
        }

        private void checkClientKeyLocality() {
            if (isClient && keyLocality == KeyLocality.LOCAL) {
                throw new IllegalStateException("The KeyLocality has been set to LOCAL, but the test is running on a client."
                        + " This doesn't make sense as no keys are stored on clients.");
            }
        }

        @Performance
        public long getOperationCount() {
            return intervalProbe.getInvocationCount();
        }

        @Override
        public void run() {
            try {
                while (!testContext.isStopped()) {
                    timeStep();
                }
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private void timeStep() throws Exception {
            ICompletableFuture<Object> future = invokeOnNextPartition();
            if (syncInvocation) {
                intervalProbe.started();
                if (syncFrequency == 1) {
                    future.get();
                } else {
                    futureList.add(future);
                    if (iteration > 0 && iteration % syncFrequency == 0) {
                        for (ICompletableFuture innerFuture : futureList) {
                            innerFuture.get();
                        }
                        futureList.clear();
                    }
                }
                intervalProbe.done();
            } else {
                future.andThen(this);
            }

            iteration++;
            if (iteration % logFrequency == 0) {
                LOGGER.info(Thread.currentThread().getName() + " at iteration: " + iteration);
            }
        }

        private ICompletableFuture<Object> invokeOnNextPartition() throws Exception {
            int partitionId = nextPartitionId();
            if (isClient) {
                SyntheticRequest request = new SyntheticRequest(syncBackupCount, asyncBackupCount, backupDelayNanos, null);
                request.setLocalPartitionId(partitionId);
                Address target = clientPartitionService.getPartitionOwner(partitionId);
                return clientInvocationService.invokeOnTarget(request, target);
            }
            SyntheticOperation operation = new SyntheticOperation(syncBackupCount, asyncBackupCount, getBackupDelayNanos());
            return operationService.invokeOnPartition(serviceName, operation, partitionId);
        }

        private int nextPartitionId() {
            return partitionSequence.get(partitionIndex++ % partitionSequence.size());
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

        @Override
        public void onResponse(Object response) {
            intervalProbe.done();
        }

        @Override
        public void onFailure(Throwable t) {
            ExceptionReporter.report(testContext.getTestId(), t);
        }

        @Override
        public void afterCompletion() {
            // nothing to do here
        }
    }

    public static void main(String[] args) throws Exception {
        SyntheticTest test = new SyntheticTest();
        new TestRunner<SyntheticTest>(test).withDuration(10).run();
    }
}
