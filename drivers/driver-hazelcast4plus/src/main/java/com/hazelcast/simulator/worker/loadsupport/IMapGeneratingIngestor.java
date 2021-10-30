package com.hazelcast.simulator.worker.loadsupport;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.logging.ILogger;

import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class IMapGeneratingIngestor<K, V> implements Ingestor {
    private final HazelcastInstance hazelcastInstance;
    private final GeneratorFunction<Long, K> keyGeneratorFn;
    private final GeneratorFunction<K, V> valueGeneratorFn;
    private final String mapName;
    private final int batchSize;
    private final int parallelism;
    private final int totalEntries;
    private final int throttle;
    private final ILogger logger;

    private IMapGeneratingIngestor(Builder<K, V> builder) {
        hazelcastInstance = requireNonNull(builder.hazelcastInstance);
        mapName = requireNonNull(builder.mapName);
        keyGeneratorFn = requireNonNull(builder.keyGeneratorFn);
        valueGeneratorFn = requireNonNull(builder.valueGeneratorFn);
        batchSize = builder.batchSize;
        parallelism = builder.parallelism;
        totalEntries = builder.totalEntries;
        throttle = builder.throttle;
        logger = hazelcastInstance.getLoggingService().getLogger(IMapGeneratingIngestor.class);
    }

    @Override
    public void ingest() throws Exception {
        IExecutorService executorService = hazelcastInstance.getExecutorService("ingest");
        Set<Member> members = hazelcastInstance.getCluster().getMembers();

        Queue<Future<Void>> futures = new LinkedList<>();
        for (int slice = 0; slice < parallelism; slice++) {
            for (Member member : members) {
                futures.add(submitToMember(executorService, member, 0, slice, futures::offer));
            }
        }

        Future<Void> future;
        while ((future = futures.poll()) != null) {
            future.get();
        }
    }

    private Future<Void> submitToMember(IExecutorService executorService, Member member, int batchSequence, int slice,
                                        Consumer<Future<Void>> futureAdderFn) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        futureAdderFn.accept(future);
        executorService.submitToMember(createIngestionTask(batchSequence, slice), member,
                new ExecutionCallback<Void>() {
                    @Override
                    public void onResponse(Void unused) {
                        if ((batchSequence + 1) * batchSize * parallelism < totalEntries) {
//                            logger.info(String.format("Submitting new task, because %d * %d < %d", (batchSequence + 1),
//                                    batchSize, totalEntries));
                            submitToMember(executorService, member, batchSequence + 1, slice, futureAdderFn);
                            future.complete(null);
                        } else {
                            future.complete(null);
                        }
                    }

                    @Override
                    public void onFailure(Throwable throwable) {
                        logger.severe(String.format("%s failed ingestion", member), throwable);
                        future.completeExceptionally(throwable);
                    }
                });
        return future;
    }

    private IMapIngestionTask<K, V> createIngestionTask(int batchSeq, int slice) {
        int entriesToSet = batchSize * parallelism;
        if ((batchSeq + 1) * batchSize * parallelism > totalEntries) {
            entriesToSet = totalEntries - (batchSeq * batchSize * parallelism);
        }
        return new IMapIngestionTask<>(batchSeq, batchSeq * batchSize * parallelism, mapName, keyGeneratorFn,
                valueGeneratorFn, entriesToSet, parallelism, throttle, slice);
    }

    public static <K, V> Builder<K, V> configure() {
        return new Builder<>();
    }

    public static class Builder<K, V> {
        private GeneratorFunction<Long, K> keyGeneratorFn;
        private GeneratorFunction<K, V> valueGeneratorFn;
        private String mapName;
        private int batchSize = 10_000;
        private int parallelism = 1;
        private int totalEntries;
        private int throttle;
        private HazelcastInstance hazelcastInstance;

        public Builder<K, V> withMapName(String mapName) {
            this.mapName = mapName;
            return this;
        }

        public Builder<K, V> withKeyGenerator(GeneratorFunction<Long, K> keyGeneratorFn) {
            this.keyGeneratorFn = keyGeneratorFn;
            return this;
        }

        public Builder<K, V> withValueGenerator(GeneratorFunction<K, V> valueGeneratorFn) {
            this.valueGeneratorFn = valueGeneratorFn;
            return this;
        }

        public Builder<K, V> withBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder<K, V> withParallelism(int parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        public Builder<K, V> withHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
            return this;
        }

        public Builder<K, V> withTotalEntries(int totalEntries) {
            this.totalEntries = totalEntries;
            return this;
        }

        public Builder<K, V> withThrottle(int throttle) {
            this.throttle = throttle;
            return this;
        }

        public void ingest() throws Exception {
            new IMapGeneratingIngestor<>(this).ingest();
        }
    }
}
