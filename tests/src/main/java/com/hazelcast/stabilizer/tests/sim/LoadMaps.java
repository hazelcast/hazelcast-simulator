package com.hazelcast.stabilizer.tests.sim;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.test.TestContext;
import com.hazelcast.stabilizer.test.annotations.Run;
import com.hazelcast.stabilizer.test.annotations.Setup;
import com.hazelcast.stabilizer.test.annotations.Verify;
import com.hazelcast.stabilizer.test.annotations.Warmup;

import java.util.Random;
import java.util.Set;

import static com.hazelcast.stabilizer.tests.helpers.HazelcastTestUtils.isMemberNode;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;


public class LoadMaps {
    private final static ILogger log = Logger.getLogger(LoadMaps.class);

    public int totalMaps = 10;
    public int totalKeys = 10;
    public int valueByteArraySize = 5000;

    public String baseMapName = this.getClass().getSimpleName();

    private String id;
    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private byte[] value;

    @Setup
    public void setup(TestContext testContex) throws Exception {
        this.testContext = testContex;
        targetInstance = testContext.getTargetInstance();
        id=testContex.getTestId();

        value = new byte[valueByteArraySize];
        Random random = new Random();
        random.nextBytes(value);
    }

    @Warmup(global = false)
    public void warmup() throws InterruptedException {
        loadDataToMaps();
    }

    public void loadDataToMaps() throws InterruptedException {
        if( isMemberNode(targetInstance) ){
            log.info(id + ": valueByteArraySize="+valueByteArraySize);
            log.info(id + ": cluster size =" + targetInstance.getCluster().getMembers().size());

            PartitionService partitionService = targetInstance.getPartitionService();
            final Set<Partition> partitionSet = partitionService.getPartitions();
            for (Partition partition : partitionSet) {
                while (partition.getOwner() == null) {
                    Thread.sleep(1000);
                }
            }
            log.info(id + ": "+partitionSet.size() + " partitions");

            Member localMember = targetInstance.getCluster().getLocalMember();

            for(int i=0; i< totalMaps; i++){
                IMap map = targetInstance.getMap(baseMapName + i);

                for(int k=0; k< totalKeys; k++){
                    Partition partition = partitionService.getPartition(k);
                    if (localMember.equals(partition.getOwner())) {
                        map.put(k, value);
                    }
                }
            }
            printMapInfo();
        }
    }

    public void printMapInfo(){
        for(int i=0; i< totalMaps; i++){
            IMap map = targetInstance.getMap(baseMapName+i);
            log.info(id + ": mapName=" + map.getName() + " size=" + map.size());
        }
    }


    @Run
    public void run() throws InterruptedException {

        while (!testContext.isStopped()) {
            Thread.sleep(2000);
        }
    }

    @Verify(global = false)
    public void verify() throws Exception {
        if(isMemberNode(targetInstance)){
            log.info(id + ": cluster size =" + targetInstance.getCluster().getMembers().size());
            printMapInfo();
        }
    }

}