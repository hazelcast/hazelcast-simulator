package com.hazelcast.simulator.tests.map;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.map.IMap;
import com.hazelcast.memory.Capacity;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.tests.map.helpers.tasks.ClearTsDirectoryTask;
import com.hazelcast.simulator.tests.map.helpers.tasks.GetHybridLogLengthTask;
import com.hazelcast.simulator.tests.map.helpers.tasks.GetMapConfigTask;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Assert;

import java.util.Random;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.jet.core.test.JetAssert.assertTrue;
import static com.hazelcast.memory.MemoryUnit.BYTES;
import static com.hazelcast.simulator.tests.helpers.KeyLocality.SHARED;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntKeys;
import static com.hazelcast.simulator.utils.GeneratorUtils.generateByteArray;

/**
 * This test is running as part of release verification simulator test. Hence every change in this class should be
 * discussed with QE team since it can affect release verification tests.
 */
public class TieredStoreMapTest extends HazelcastTest {
    // properties
    public int keyDomain = 2_000_000;
    public int minValueByteArrayLength = 1;
    public int maxValueByteArrayLength = 150_000;
    public boolean clearTsDirectoryOnPrepare = true;
    public boolean fillOnPrepare = true;
    public boolean destroyOnExit = true;

    public KeyLocality keyLocality = SHARED;

    private IMap<Integer, byte[]> map;
    private int[] keys;
    private IExecutorService executor;

    @Setup
    public void setUp() throws ReflectiveOperationException {
        keys = generateIntKeys(keyDomain, keyLocality, targetInstance);
        executor = targetInstance.getExecutorService(name);
        map = targetInstance.getMap(name);
        Assert.assertTrue("Disk Tier Config must be enabled for map: " + name, isTsEnabledForMap());
    }

    @Prepare(global = true)
    public void prepare() {
        if (clearTsDirectoryOnPrepare) {
            clearTsDirectory();
        }
        if (fillOnPrepare) {
            fillMap();
        }
    }

    @Verify
    public void verify() {
        Capacity hybridLogLength = Capacity.of(getHybridLogLengthSum(), BYTES);
        logger.info("Sum of hybrid log length from all members: " + hybridLogLength.toPrettyString());
        assertTrue("Hybrid log is equal to 0. Tiered Storage was not used.", hybridLogLength.bytes() > 0);
    }

    @Teardown(global = true)
    public void tearDown() {
        if (destroyOnExit) {
            map.destroy();
        }
        executor.shutdown();
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

    private boolean isTsEnabledForMap() {
        return getMapConfig().getTieredStoreConfig().getDiskTierConfig().isEnabled();
    }

    private MapConfig getMapConfig() {
        //Get MapConfig from the cluster, default option with client has lack of required fields
        try {
            return executor.submit(new GetMapConfigTask(name)).get();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException("GetMapConfigTask failed", e);
        }
    }

    private void fillMap() {
        Random random = new Random();
        Streamer<Integer, byte[]> streamer = StreamerFactory.getInstance(map);
        logger.info("Starting new batch");
        for (int key : keys) {
            streamer.pushEntry(key, generateByteArray(random, RandomUtils.nextInt(minValueByteArrayLength, maxValueByteArrayLength)));
            if (key % 1_000 == 0) {
                logger.info("Added " + key + " of " + keys.length + " keys to Streamer");
            }
        }
        streamer.await();
    }

    private void clearTsDirectory() {
        executor.submitToAllMembers(new ClearTsDirectoryTask())
                .values()
                .forEach(future -> {
                    try {
                        future.get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new IllegalStateException("Error during clearing ts directory task", e);
                    }
                });
    }

    private long getHybridLogLengthSum() {
        //Aggregate hybrid log length from the members
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

    public class ThreadState extends BaseThreadState {
        private byte[] randomByteArray(int length) {
            byte[] result = new byte[length];
            random.nextBytes(result);
            return result;
        }

        private Integer randomKey() {
            return randomInt(keyDomain);
        }

        private byte[] randomValue() {
            return randomByteArray(RandomUtils.nextInt(minValueByteArrayLength, maxValueByteArrayLength));
        }
    }


}
