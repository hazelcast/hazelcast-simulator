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
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.BeforeRun;
import com.hazelcast.simulator.test.annotations.StartNanos;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.tests.helpers.HazelcastTestUtils;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.tests.helpers.KeyUtils;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.spi.OperationService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getPartitionDistributionInformation;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isClient;
import static com.hazelcast.simulator.tests.helpers.KeyLocality.SHARED;

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
    public byte syncBackupCount = 0;
    public byte asyncBackupCount = 1;
    public long backupDelayNanos = 1000 * 1000;
    public boolean randomizeBackupDelay = true;
    public KeyLocality keyLocality = SHARED;
    public int keyCount = 1000;
    public String serviceName;

    @BeforeRun
    public void beforeRun(ThreadState state) {
        if (isClient(targetInstance)) {
            throw new IllegalArgumentException("SyntheticTest doesn't support clients at the moment");
        }

        state.operationService = HazelcastTestUtils.getOperationService(targetInstance);

        int[] keys = KeyUtils.generateIntKeys(keyCount, keyLocality, targetInstance);
        for (int key : keys) {
            Partition partition = targetInstance.getPartitionService().getPartition(key);
            state.partitionSequence.add(partition.getPartitionId());
        }
        Collections.shuffle(state.partitionSequence);
    }

    @TimeStep(prob = 1)
    public void invoke(ThreadState state) throws Exception {
        ICompletableFuture<Object> future = state.invokeOnNextPartition();
        future.get();
    }

    @TimeStep(prob = 0)
    public void invokeAsync(ThreadState state, final Probe probe, @StartNanos final long startNanos) throws Exception {
        ICompletableFuture<Object> future = state.invokeOnNextPartition();
        future.andThen(new ExecutionCallback<Object>() {
            @Override
            public void onResponse(Object o) {
                probe.done(startNanos);
            }

            @Override
            public void onFailure(Throwable throwable) {
                ExceptionReporter.report(testContext.getTestId(), throwable);
            }
        });
    }

    public class ThreadState extends BaseThreadState {

        private final List<Integer> partitionSequence = new ArrayList<Integer>();
        private final Random random = new Random();

        private OperationService operationService;

        private int partitionIndex;

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
    }

    @Teardown
    public void teardown() {
        logger.info(getOperationCountInformation(targetInstance));
        logger.info(getPartitionDistributionInformation(targetInstance));
    }
}
