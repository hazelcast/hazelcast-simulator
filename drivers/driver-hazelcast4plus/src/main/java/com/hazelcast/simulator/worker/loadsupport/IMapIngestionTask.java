package com.hazelcast.simulator.worker.loadsupport;

import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;

import java.io.Serializable;
import java.util.BitSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.LockSupport;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class IMapIngestionTask<K, V> implements Callable<Void>, HazelcastInstanceAware, Serializable,
        MembershipListener {
    private final int batchSeq;
    private final int startSeq;
    private final String mapName;
    private final GeneratorFunction<Long, K> keyGeneratorFn;
    private final GeneratorFunction<K, V> valueGeneratorFn;
    private final int entriesToSet;
    private final int parallelism;
    private final int throttle;
    private final int slice;
    private transient volatile IngestionContext ingestionContext;

    public IMapIngestionTask(int batchSeq, int startSeq, String mapName, GeneratorFunction<Long, K> keyGeneratorFn,
                             GeneratorFunction<K, V> valueGeneratorFn, int entriesToSet, int parallelism,
                             int throttle, int slice) {
        this.batchSeq = batchSeq;
        this.startSeq = startSeq;
        this.mapName = requireNonNull(mapName);
        this.keyGeneratorFn = requireNonNull(keyGeneratorFn);
        this.valueGeneratorFn = requireNonNull(valueGeneratorFn);
        this.entriesToSet = entriesToSet;
        this.parallelism = parallelism;
        this.throttle = throttle;
        this.slice = slice;
    }

    @Override
    public Void call() throws Exception {
        final IngestionContext<K, V> ingestionContext = this.ingestionContext;
//        ingestionContext.logger.info("Data ingestion task " + batchSeq + " started to insert " + entriesToSet
//                + " entries");

        try {
            int inserted = 0;
            int ignored = 0;
            for (long i = slice; i < entriesToSet; i += parallelism) {
                K key = keyGeneratorFn.apply(startSeq + i);

                if (!ingestionContext.clusterMembersStable) {
                    throw new IllegalStateException("Change in cluster topology is not allowed during data ingestion");
                }

                int partitionId = ingestionContext.partitionService.getPartition(key).getPartitionId();

                if (ingestionContext.ownedPartitions.get(partitionId)) {
                    ingestionContext.throttlingSemaphore.acquire();

                    V value = valueGeneratorFn.apply(key);
                    ingestionContext.map.putAsync(key, value)
                            .thenRun(() -> ingestionContext.throttlingSemaphore.release())
                            .exceptionally(throwable -> {
                                ingestionContext.throttlingSemaphore.release();
                                return null;
                            });
                    inserted++;
                } else {
                    ignored++;
                }
            }

            int size = ingestionContext.map.size();
            ingestionContext.logger.info("Data ingestion task " + batchSeq + ":" + slice + " finished. Map size: " + size +
                    ". Inserted: " + inserted + ", ignored: " + ignored);

            return null;
        } finally {
            ingestionContext.cluster.removeMembershipListener(ingestionContext.membershipListenerUuid);
        }
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        IngestionContext<K, V> newIngestionContext = new IngestionContext<>(throttle);
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

    private static BitSet getOwnedPartitions(Set<Partition> partitions, ILogger logger, Member localMember) {
        BitSet ownedPartitions = new BitSet(partitions.size());
        for (Partition partition : partitions) {
            Member owner;
            while ((owner = partition.getOwner()) == null) {
                logger.warning("Owner of partition " + partition.getPartitionId() + " is null. Waiting for the partition "
                        + "assignment to complete.");
                LockSupport.parkNanos(SECONDS.toNanos(1));
            }

            if (owner.equals(localMember)) {
                ownedPartitions.set(partition.getPartitionId());
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
        IngestionContext<K, V> ingestionContext = this.ingestionContext;
        ingestionContext.logger.severe("Cluster membership changed during data ingestion, failing the ingestion process. The "
                + "captured event: " + membershipEvent);
        ingestionContext.clusterMembersStable = false;
    }

    @Override
    public String toString() {
        return "IMapIngestionTask{"
                + "taskSequence=" + batchSeq
                + ", mapName='" + mapName + '\''
                + ", keyGeneratorFn=" + keyGeneratorFn
                + ", valueGeneratorFn=" + valueGeneratorFn
                + ", entriesToSet=" + entriesToSet
                + ", parallelism=" + parallelism
                + ", throttle=" + throttle
                + ", ingestionContext=" + ingestionContext
                + '}';
    }

    private static final class IngestionContext<K, V> {
        private ILogger logger;
        private IMap<K, V> map;
        private Semaphore throttlingSemaphore;
        private BitSet ownedPartitions;
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
