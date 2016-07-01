/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.tests.synthetic;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Partition;
import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.annotations.InjectProbe;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.tests.AbstractTest;
import com.hazelcast.simulator.tests.helpers.HazelcastTestUtils;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.tests.helpers.KeyUtils;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.worker.tasks.IWorker;
import com.hazelcast.spi.OperationService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getPartitionDistributionInformation;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isClient;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.rethrow;

/**
 * The SyntheticTest can be used to test features like back pressure.
 * <p>
 * It can be configured with:
 * - sync invocation
 * - async invocation
 * - number of sync backups
 * - number of async backups
 * - delay of back pressing.
 * <p>
 * This test doesn't make use of any normal data-structures like an {@link com.hazelcast.core.IMap}, but uses the SPI directly to
 * execute operations and backups. This gives a lot of control on the behavior.
 * <p>
 * If for example we want to test back pressure on async backups, just set the asyncBackupCount to a value larger than 0 and if
 * you want to simulate a slow down, also set the backupDelayNanos. If this is set to a high value, on the backup you will get a
 * pileup of back up commands which eventually can lead to an OOME.
 * <p>
 * Another interesting scenario to test is a normal async invocation of a readonly operation (so no async/sync-backups) and see if
 * the system can be flooded with too many request. Normal sync operations don't cause that many problems because there is a
 * natural balance between the number of threads and the number of pending invocations.
 */
public class SyntheticTest extends AbstractTest {

    // properties
    public boolean syncInvocation = true;
    public byte syncBackupCount = 0;
    public byte asyncBackupCount = 1;
    public long backupDelayNanos = 1000 * 1000;
    public boolean randomizeBackupDelay = true;
    public KeyLocality keyLocality = KeyLocality.SHARED;
    public int keyCount = 1000;
    public int syncFrequency = 1;
    public String serviceName;

    @InjectProbe(useForThroughput = true)
    private Probe probe;

    @Teardown
    public void teardown() {
        logger.info(getOperationCountInformation(targetInstance));
        logger.info(getPartitionDistributionInformation(targetInstance));
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker implements IWorker, ExecutionCallback<Object> {

        private final List<Integer> partitionSequence = new ArrayList<Integer>();
        private final List<ICompletableFuture> futureList = new ArrayList<ICompletableFuture>(syncFrequency);
        private final Random random = new Random();

        private final OperationService operationService;

        private int partitionIndex;
        private long iteration;

        public Worker() {
            if (isClient(targetInstance)) {
                throw new IllegalArgumentException("SyntheticTest doesn't support clients at the moment");
            }

            operationService = HazelcastTestUtils.getOperationService(targetInstance);

            int[] keys = KeyUtils.generateIntKeys(keyCount, keyLocality, targetInstance);
            for (int key : keys) {
                Partition partition = targetInstance.getPartitionService().getPartition(key);
                partitionSequence.add(partition.getPartitionId());
            }
            Collections.shuffle(partitionSequence);
        }

        @Override
        public void run() {
            try {
                while (!testContext.isStopped()) {
                    timeStep();
                }
            } catch (Exception e) {
                throw rethrow(e);
            }
        }

        private void timeStep() throws Exception {
            ICompletableFuture<Object> future = invokeOnNextPartition();
            if (syncInvocation) {
                long started = System.nanoTime();
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
                probe.recordValue(System.nanoTime() - started);
            } else {
                future.andThen(this);
            }

            iteration++;
        }

        private ICompletableFuture<Object> invokeOnNextPartition() throws Exception {
            int partitionId = nextPartitionId();
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
            probe.done(0);
        }

        @Override
        public void onFailure(Throwable t) {
            ExceptionReporter.report(testContext.getTestId(), t);
        }

        @Override
        public void afterCompletion() {
            // nothing to do here
        }

        @Override
        public void beforeRun() throws Exception {
            // nothing to do here
        }

        @Override
        public void afterRun() throws Exception {
            // nothing to do here
        }
    }

}
