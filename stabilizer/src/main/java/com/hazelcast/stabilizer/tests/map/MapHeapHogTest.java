package com.hazelcast.stabilizer.tests.map;


import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import java.util.concurrent.TimeUnit;

public class MapHeapHogTest {

    public String basename = this.getClass().getName();
    public int threadCount = 3;
    public int ttlHours = 24;
    public double approxHeapUsageFactor = 0.9;

    private TestContext testContext;
    private HazelcastInstance targetInstance;

    private long approxEntryBytesSize = 238;

    private IMap map;

    public MapHeapHogTest(){ }

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();

        map = targetInstance.getMap(basename);
    }

    @Warmup(global = false)
    public void warmup() {
        if(isMemberNode(targetInstance)){
            printMemStats();

            long free = Runtime.getRuntime().freeMemory();
            long total =  Runtime.getRuntime().totalMemory();
            long used = total - free;
            long max =  Runtime.getRuntime().maxMemory();
            long totalFree = max - used;

            long maxLocalEntries = (long) ( (totalFree / approxEntryBytesSize) * approxHeapUsageFactor) ;

            long key=0;
            for(int i=0; i<maxLocalEntries; i++){
                key = nextKeyOwnedby(key, targetInstance);
                map.put(key, key, ttlHours, TimeUnit.HOURS);
                key++;
            }
            System.out.println(basename+" map size = "+map.size());
            System.out.println(basename+" putCount = "+maxLocalEntries);

            printMemStats();
        }
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    private class Worker implements Runnable {
        @Override
        public void run() {
            while (!testContext.isStopped()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Verify(global = false)
    public void loaclVerify() throws Exception {

        if(isMemberNode(targetInstance)){
            printMemStats();
        }

    }

    private double heapUsedFactor() {
        long total = Runtime.getRuntime().totalMemory();
        long free = Runtime.getRuntime().freeMemory();
        long used = total - free;
        long max = Runtime.getRuntime().maxMemory();
        return used / max;
    }

    public void printMemStats(){

        long free = Runtime.getRuntime().freeMemory();
        long total =  Runtime.getRuntime().totalMemory();
        long used = total - free;
        long max =  Runtime.getRuntime().maxMemory();
        double usedOfMax = 100.0 * ( (double) used / (double) max);

        long totalFree =  max - used;

        System.out.println(basename+" free = "+humanReadableByteCount(free, true)+" = "+free);
        System.out.println(basename+" total free = "+humanReadableByteCount(totalFree, true)+" = "+totalFree);
        System.out.println(basename+" used = "+humanReadableByteCount(used, true)+" = "+ used);
        System.out.println(basename+" max = "+humanReadableByteCount(max, true)+" = "+max);
        System.out.println(basename+" usedOfMax = "+usedOfMax+"%");
        System.out.println();
    }

    public static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp-1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

    public static long nextKeyOwnedby(long key, HazelcastInstance instance) {
        final Member localMember = instance.getCluster().getLocalMember();
        final PartitionService partitionService = instance.getPartitionService();
        for ( ; ; ) {

            Partition partition = partitionService.getPartition(key);
            if (localMember.equals(partition.getOwner())) {
                return key;
            }
            key++;
        }
    }

    public static boolean isMemberNode(HazelcastInstance instance){
        return instance instanceof HazelcastInstanceProxy;
    }

}
