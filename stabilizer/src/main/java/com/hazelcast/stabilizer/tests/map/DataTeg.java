package com.hazelcast.stabilizer.tests.map;


import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.PartitionServiceProxy;
import com.hazelcast.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;

public class DataTeg {


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

    }

    @Warmup(global = true)
    public void warmup() throws InterruptedException {

        IMap map = targetInstance.getMap(basename);

        for(int i=0; i<maxItems; i++){
            map.put(i, i);
        }


        /*
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
                    System.out.println(basename+"owner: " + partition.getOwner().getHost());
                    System.out.println(basename+"back : " + partition.getReplicaAddress(1).getHost());
                }
            }
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        */


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

        int max=0;
        while(map.size() != maxItems){

            System.out.println(basename+": verify map size ="+ map.size() +" target = "+maxItems );
            Thread.sleep(10000);

            if(max++==5){
                break;
            }
        }

        assertEquals("data loss", map.size(), maxItems);
        System.out.println(basename+"verify OK "+map.size()+"=="+maxItems);
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

}
