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

package com.hazelcast.simulator.worker.loadsupport;

import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import org.apache.commons.lang3.RandomUtils;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Executor task for ingesting data from inside Hazelcast members, bypassing
 * the clients. This task is mainly suited for ingesting big amount of data
 * faster that would otherwise take too long if done through the clients,
 * involving network communication. Instead, this task iterates through the
 * key domain and puts entries that are stored on local partitions with
 * <strong>random</strong> values.
 *
 * In order to guarantee that the expected amount of entries are put into
 * the map after the end of the ingestion, this ingestion method relies on
 * stable cluster membership. Therefore, it subscribes to membership change
 * events and fails the ingestion if a change in the cluster membership
 * is detected.
 */
public class IMapIngestionTask implements Callable<Void>, HazelcastInstanceAware, Serializable, MembershipListener {
    public static final int PERCENT_MULTIPLIER = 100;
    private final int minValueLength;
    private final int maxValueLength;
    private final int keyDomain;
    private final String mapName;
    private final int throttle;
    private final int taskSequence;
    private final int increment;
    private transient volatile IngestionContext ingestionContext;

    public IMapIngestionTask(String mapName, int minValueLength, int maxValueLength, int keyDomain, int throttle,
                             int taskSequence, int increment) {
        this.mapName = mapName;
        this.minValueLength = minValueLength;
        this.maxValueLength = maxValueLength;
        this.keyDomain = keyDomain;
        this.throttle = throttle;
        this.taskSequence = taskSequence;
        this.increment = increment;
    }

    /**
     * Start data ingestion.
     *
     * @param hazelcastInstance The instance through which data ingestion to be initiated.
     * @param mapName           The name of the map to be populated.
     * @param parallelism       The number of parallel tasks to be executed per member.
     * @param throttle          The maximum number of IMap puts in-flight.
     * @param keyDomain         The size of the key domain.
     * @param minValueLength    The minimum size of the values in bytes to put into the IMap.
     * @param maxValueLength    The maximum size of the values in bytes to put into the IMap.
     * @return list of the futures of the submitted ingestion tasks
     */
    public static List<Future<Void>> ingest(HazelcastInstance hazelcastInstance, String mapName, int parallelism,
                                            int throttle, int keyDomain, int minValueLength, int maxValueLength) {

        IExecutorService executorService = hazelcastInstance.getExecutorService("ingest");
        List<Future<Void>> futures = new LinkedList<>();

        for (int taskSequence = 0; taskSequence < parallelism; taskSequence++) {
            IMapIngestionTask ingestionTask = new IMapIngestionTask(mapName, minValueLength, maxValueLength, keyDomain, throttle,
                    taskSequence, parallelism);
            Map<Member, Future<Void>> futureMap = executorService.submitToAllMembers(ingestionTask);

            for (Future<Void> future : futureMap.values()) {
                futures.add(future);
            }
        }

        return futures;
    }

    @Override
    public Void call() throws Exception {
        final IngestionContext ingestionContext = this.ingestionContext;

        ingestionContext.logger.info("Data ingestion task " + taskSequence + " started");
        int lastProgressPercent = 0;

        byte[] value = createValue(minValueLength, maxValueLength);
        for (long key = taskSequence; key < keyDomain; key += increment) {

            if (!ingestionContext.clusterMembersStable) {
                throw new IllegalStateException("Change in cluster topology is not allowed during data ingestion");
            }

            lastProgressPercent = trackProgress(lastProgressPercent, key);

            int partitionId = ingestionContext.partitionService.getPartition(key).getPartitionId();

            if (ingestionContext.ownedPartitions[partitionId]) {
                ingestionContext.throttlingSemaphore.acquire();

                ingestionContext.map.putAsync(key, value)
                                    .thenRun(() -> ingestionContext.throttlingSemaphore.release())
                                    .exceptionally(throwable -> {
                                        ingestionContext.throttlingSemaphore.release();
                                        return null;
                                    });
            }
        }

        ingestionContext.cluster.removeMembershipListener(ingestionContext.membershipListenerUuid);

        int size = ingestionContext.map.size();
        ingestionContext.logger.info("Data ingestion task " + taskSequence + " finished. Map size: " + size);

        return null;
    }

    private int trackProgress(int lastPercent, double key) {
        int progressPercent;
        progressPercent = (int) (key / keyDomain * PERCENT_MULTIPLIER);

        if (taskSequence == 0 && lastPercent != progressPercent) {
            lastPercent = progressPercent;
            int size = ingestionContext.map.size();
            ingestionContext.logger.info("Progress: " + progressPercent + "% - map size: " + size);
        }
        return lastPercent;
    }

    private static byte[] createValue(int minValueLength, int maxValueLength) {
        int length = RandomUtils.nextInt(minValueLength, maxValueLength);
        byte[] value = new byte[length];
        ThreadLocalRandom.current().nextBytes(value);
        return value;
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        IngestionContext newIngestionContext = new IngestionContext(throttle);
        newIngestionContext.logger = hazelcastInstance.getLoggingService().getLogger(IMapIngestionTask.class);
        newIngestionContext.localMember = hazelcastInstance.getCluster().getLocalMember();
        newIngestionContext.partitionService = hazelcastInstance.getPartitionService();
        newIngestionContext.ownedPartitions = getOwnedPartitions(newIngestionContext.partitionService.getPartitions(),
                newIngestionContext.logger, newIngestionContext.localMember);
        newIngestionContext.map = hazelcastInstance.getMap(mapName);

        newIngestionContext.cluster = hazelcastInstance.getCluster();
        newIngestionContext.membershipListenerUuid = newIngestionContext.cluster.addMembershipListener(this);

        this.ingestionContext = newIngestionContext;
    }

    private static boolean[] getOwnedPartitions(Set<Partition> partitions, ILogger logger, Member localMember) {
        boolean[] ownedPartitions = new boolean[partitions.size()];
        for (Partition partition : partitions) {
            Member owner;
            while ((owner = partition.getOwner()) == null) {
                logger.warning("Owner of partition " + partition.getPartitionId() + " is null. Waiting for the partition "
                        + "assignment to complete.");
                LockSupport.parkNanos(SECONDS.toNanos(1));
            }

            if (owner.equals(localMember)) {
                ownedPartitions[partition.getPartitionId()] = true;
            }
        }
        return ownedPartitions;
    }

    @Override
    public void memberAdded(MembershipEvent membershipEvent) {
        handleClusterMembershipChange(membershipEvent);
    }

    @Override
    public void memberRemoved(MembershipEvent membershipEvent) {
        handleClusterMembershipChange(membershipEvent);
    }

    private void handleClusterMembershipChange(MembershipEvent membershipEvent) {
        IngestionContext ingestionContext = this.ingestionContext;
        ingestionContext.logger.severe("Cluster membership changed during data ingestion, failing the ingestion process. The "
                + "captured event: " + membershipEvent);
        ingestionContext.clusterMembersStable = false;
    }

    private static final class IngestionContext {
        private ILogger logger;
        private IMap<Long, byte[]> map;
        private Semaphore throttlingSemaphore;
        private boolean[] ownedPartitions;
        private PartitionService partitionService;
        private Member localMember;
        private boolean clusterMembersStable = true;
        private Cluster cluster;
        private UUID membershipListenerUuid;

        private IngestionContext(int throttle) {
            throttlingSemaphore = new Semaphore(throttle);

        }
    }
}
