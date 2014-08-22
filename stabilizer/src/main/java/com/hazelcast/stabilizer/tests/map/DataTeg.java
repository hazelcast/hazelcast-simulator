package com.hazelcast.stabilizer.tests.map;


import com.hazelcast.client.proxy.PartitionServiceProxy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.Performance;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

public class DataTeg {

    private final static ILogger log = Logger.getLogger(DataTeg.class);

    public String basename = this.getClass().getName();
    public int maxItems=10000;

    public int clusterSize=6;

    private TestContext testContext;
    private HazelcastInstance targetInstance;

    public DataTeg(){ }

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();

    }

    @Warmup(global = false)
    public void warmup() throws InterruptedException {

        while ( targetInstance.getCluster().getMembers().size() != clusterSize ){
            System.out.println(basename+" waiting cluster == "+clusterSize);
            Thread.sleep(1000);
        }
        final PartitionService ps = targetInstance.getPartitionService();
        for (Partition partition : ps.getPartitions()) {
            while (partition.getOwner() == null) {
                System.out.println(basename+" partition owner ?");
                Thread.sleep(1000);
            }
        }

        IMap map = targetInstance.getMap(basename);

        for(int i=0; i<maxItems; i++){
            map.put(i, i);
        }


        try {
            final PartitionServiceProxy partitionService = (PartitionServiceProxy) targetInstance.getPartitionService();
            final Field field = PartitionServiceProxy.class.getDeclaredField("partitionService");
            field.setAccessible(true);
            final InternalPartitionServiceImpl internalPartitionService = (InternalPartitionServiceImpl) field.get(partitionService);
            final Field partitionsField = InternalPartitionServiceImpl.class.getDeclaredField("partitions");
            partitionsField.setAccessible(true);
            final InternalPartition[] partitions = (InternalPartition[]) partitionsField.get(internalPartitionService);

            for (InternalPartition partition : partitions) {

                if(partition.getOwner().getHost().equals(partition.getReplicaAddress(1).getHost())){


                    System.out.println(basename+"----------------ERROR---------------------------------");
                    System.out.println(basename+"owner: " + partition.getOwner().getPort());
                    System.out.println(basename+"back : " + partition.getReplicaAddress(1).getPort());
                }
            }
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }


    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        spawner.spawn(new Worker());
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
    public void verify() throws Exception {

        IMap map = targetInstance.getMap(basename);

        for(int i=0; i<3; i++){
            System.out.println(basename+": map size ="+ map.size() +" target = "+maxItems );
            Thread.sleep(3000);
        }
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
