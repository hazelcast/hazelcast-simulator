package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.Pipelining;
import com.hazelcast.map.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.StartNanos;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.worker.loadsupport.GeneratorFunction;
import com.hazelcast.simulator.worker.loadsupport.IMapGeneratingIngestor;
import org.apache.commons.lang3.RandomUtils;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;

import static com.hazelcast.simulator.utils.GeneratorUtils.generateByteArrays;

public class LongByteArrayBigMapTest extends HazelcastTest {

    // properties
    public int keyDomain = 10000;
    public int valueCount = 10000;
    public int minValueLength = 10;
    public int maxValueLength = 10;
    public int pipelineDepth = 10;
    public int pipelineIterations = 100;
    public int getAllSize = 5;
    public int ingestionThrottle = 100;
    public int ingestionParallelism = 4;
    public int ingestionBatchSize = 1000;

    private IMap<Long, byte[]> map;
    private byte[][] values;
    private final Executor callerRuns = Runnable::run;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
        values = generateByteArrays(valueCount, minValueLength, maxValueLength);
    }

    @Prepare(global = true)
    public void prepare() throws Exception {
        IMapGeneratingIngestor.<Long, byte[]>configure()
                .withHazelcastInstance(targetInstance)
                .withMapName(map.getName())
                .withBatchSize(ingestionBatchSize)
                .withThrottle(ingestionThrottle)
                .withParallelism(ingestionParallelism)
                .withTotalEntries(keyDomain)
                .withKeyGenerator(GeneratorFunction.identity())
                .withValueGenerator(GeneratorFunction.randomBytes(minValueLength, maxValueLength))
                .ingest();
    }

    @TimeStep(prob = -1)
    public byte[] get(ThreadState state) {
        return map.get(state.randomKey());
    }

    @TimeStep(prob = -1)
    public Map<Long, byte[]> getAll(ThreadState state) {
        Set<Long> keys = new HashSet<>();
        for (int k = 0; k < getAllSize; k++) {
            keys.add(state.randomKey());
        }
        return map.getAll(keys);
    }

    @TimeStep(prob = 0)
    public CompletableFuture getAsync(ThreadState state) {
        return map.getAsync(state.randomKey()).toCompletableFuture();
    }

    @TimeStep(prob = 0.1)
    public byte[] put(ThreadState state) {
        return map.put(state.randomKey(), state.randomValue());
    }

    @TimeStep(prob = 0.0)
    public CompletableFuture putAsync(ThreadState state) {
        return map.putAsync(state.randomKey(), state.randomValue()).toCompletableFuture();
    }

    @TimeStep(prob = 0)
    public void set(ThreadState state) {
        map.set(state.randomKey(), state.randomValue());
    }

    @TimeStep(prob = 0)
    public CompletableFuture setAsync(ThreadState state) {
        return map.setAsync(state.randomKey(), state.randomValue()).toCompletableFuture();
    }

    @TimeStep(prob = 0)
    public void pipelinedGet(final ThreadState state, @StartNanos final long startNanos, final Probe probe) throws Exception {
        if (state.pipeline == null) {
            state.pipeline = new Pipelining<>(pipelineDepth);
        }

        CompletableFuture<byte[]> f = map.getAsync(state.randomKey()).toCompletableFuture();
        f.whenCompleteAsync((bytes, throwable) -> probe.done(startNanos), callerRuns);
        state.pipeline.add(f);
        state.i++;
        if (state.i == pipelineIterations) {
            state.i = 0;
            state.pipeline.results();
            state.pipeline = null;
        }
    }

    private static byte[] createValue(int minValueLength, int maxValueLength) {
        int length = RandomUtils.nextInt(minValueLength, maxValueLength);
        byte[] value = new byte[length];
        ThreadLocalRandom.current().nextBytes(value);
        return value;
    }

    public class ThreadState extends BaseThreadState {
        private Pipelining<byte[]> pipeline;
        private int i;

        private long randomKey() {
            return randomLong(keyDomain);
        }

        private byte[] randomValue() {
            return values[randomInt(values.length)];
        }
    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }
}
