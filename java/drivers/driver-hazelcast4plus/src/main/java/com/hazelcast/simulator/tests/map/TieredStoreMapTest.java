package com.hazelcast.simulator.tests.map;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.map.IMap;
import com.hazelcast.memory.Capacity;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.*;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.tests.map.helpers.tasks.GetHybridLogLengthTask;
import com.hazelcast.simulator.tests.map.helpers.tasks.GetMapConfigTask;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import org.junit.Assert;

import java.util.Random;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.jet.core.test.JetAssert.assertTrue;
import static com.hazelcast.memory.MemoryUnit.BYTES;
import static com.hazelcast.simulator.tests.helpers.KeyLocality.SHARED;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntKeys;
import static com.hazelcast.simulator.utils.GeneratorUtils.generateByteArray;


public class TieredStoreMapTest extends HazelcastTest {
    // properties
    public int keyCount = 200_000;
    public int valueByteArrayLength = 512_100;
    public boolean fillOnPrepare = true;
    public int fillOnPrepareAwaitBatchSize = 10_000;
    public boolean destroyOnExit = true;
    public KeyLocality keyLocality = SHARED;

    private IMap<Integer, byte[]> map;
    private int[] keys;
    private IExecutorService executor;

    @Setup
    public void setUp() throws ReflectiveOperationException {
        map = targetInstance.getMap(name);
        keys = generateIntKeys(keyCount, keyLocality, targetInstance);
        executor = targetInstance.getExecutorService(name);

        Assert.assertTrue("Disk Tier Config must be enabled for map: " + name, getMapConfig().getTieredStoreConfig().getDiskTierConfig().isEnabled());

        //Estimated total size without overheads and backups - keys + values size
        Capacity estimatedDatasetSize = Capacity.of(keyCount * 4L + (long) keyCount * valueByteArrayLength, BYTES);
        logger.info("Estimated total dataset size (without overheads and backups): " + estimatedDatasetSize.toPrettyString());
    }

    private MapConfig getMapConfig() {
        try {
            return executor.submit(new GetMapConfigTask(name)).get();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException("GetMapConfigTask failed", e);
        }
    }

    private long getHybridLogLength() {
        return executor.submitToAllMembers((new GetHybridLogLengthTask()))
                .entrySet()
                .stream()
                .map(memberFutureEntry -> {
                    try {
                        return memberFutureEntry.getValue().get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new IllegalStateException("GetHlogLengthTask failed for member" + memberFutureEntry.getKey().getAddress().getHost(), e);
                    }
                })
                .reduce(Long::sum)
                .orElseThrow(() -> new IllegalStateException("GetHlogLengthTask failed - submitToAllMembers returned empty map"));
    }

    @Prepare
    public void prepare() {
        if (!fillOnPrepare) {
            return;
        }
        Random random = new Random();

        Streamer<Integer, byte[]> streamer = StreamerFactory.getInstance(map);
        for (int key : keys) {
            streamer.pushEntry(key, generateByteArray(random, valueByteArrayLength));
            //Await for batches, to prevent OOM on worker side
            if (key % fillOnPrepareAwaitBatchSize == 0) {
                streamer.await();
            }
        }
        streamer.await();
    }

    @TimeStep(prob = -1)
    public void get(ThreadState state) {
        map.get(state.randomKey());
    }

    @TimeStep(prob = 0.1)
    public void put(ThreadState state) {
        map.put(state.randomKey(), state.randomValue());
    }

    @TimeStep(prob = 0.1)
    public void putIfAbsent(ThreadState state) {
        map.putIfAbsent(state.randomKey(), state.randomValue());
    }

    @TimeStep(prob = 0.1)
    public void set(ThreadState state) {
        map.set(state.randomKey(), state.randomValue());
    }

    @TimeStep(prob = 0.1)
    public void remove(ThreadState state) {
        map.remove(state.randomKey());
    }

    @TimeStep(prob = 0.1)
    public void delete(ThreadState state) {
        map.delete(state.randomKey());
    }

    @TimeStep(prob = 0.1)
    public void setThenDelete(ThreadState state) {
        map.set(state.randomKey(), state.randomValue());
        map.delete(state.randomKey());
    }

    public class ThreadState extends BaseThreadState {
        private byte[] randomByteArray(int length) {
            byte[] result = new byte[length];
            random.nextBytes(result);
            return result;
        }

        private Integer randomKey() {
            return randomInt(keyCount);
        }

        private byte[] randomValue() {
            return randomByteArray(valueByteArrayLength);
        }
    }

    @Verify()
    public void verify() {
        Capacity hybridLogLength = Capacity.of(getHybridLogLength(), BYTES);
        logger.info("Sum of hybrid log from all members: " + hybridLogLength.toPrettyString());
        assertTrue("Hybrid log equal to 0. Tiered Storage was not used.", hybridLogLength.bytes() > 0);
    }

    @Teardown
    public void tearDown() {
        if (destroyOnExit) {
            map.destroy();
        }
        executor.shutdown();
    }
}
