package com.hazelcast.stabilizer.tests.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

public class MapDataIntegrityTest {
    private final static ILogger log = Logger.getLogger(MapDataIntegrityTest.class);

    public int MapIntegrityThreadCount=8;
    public int stressThreadCount=8;
    public int totalIntegritiyKeys=10000;
    public int totalStressKeys=1000;
    public int valueSize=1000;
    public boolean mapLoad=true;
    public boolean doRunAsserts = true;
    public String basename = this.getClass().getCanonicalName();

    private String id;
    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private IMap<Integer, byte[]> integrityMap;
    private IMap<Integer, byte[]> stressMap;
    private byte[] value;

    MapIntegrityThread[] integrityThreads;


    @Setup
    public void setup(TestContext testContex) throws Exception {
        this.testContext = testContex;
        targetInstance = testContext.getTargetInstance();
        id=testContex.getTestId();
        integrityMap = targetInstance.getMap(basename+"Integrity");
        stressMap = targetInstance.getMap(basename+"Stress");

        integrityThreads = new MapIntegrityThread[MapIntegrityThreadCount];

        value = new byte[valueSize];
        Random random = new Random();
        random.nextBytes(value);

        if(mapLoad && TestUtils.isMemberNode(targetInstance)){

            PartitionService partitionService = targetInstance.getPartitionService();
            final Set<Partition> partitionSet = partitionService.getPartitions();
            for (Partition partition : partitionSet) {
                while (partition.getOwner() == null) {
                    Thread.sleep(1000);
                }
            }
            log.info(id + ": "+partitionSet.size() + " partitions");

            Member localMember = targetInstance.getCluster().getLocalMember();
            for(int i=0; i< totalIntegritiyKeys; i++){
                Partition partition = partitionService.getPartition(i);
                if (localMember.equals(partition.getOwner())) {
                    integrityMap.put(i, value);
                }
            }
            log.info(id + ": integrityMap=" + integrityMap.getName() + " size=" + integrityMap.size());

            Config config = targetInstance.getConfig();
            MapConfig mapConfig = config.getMapConfig(integrityMap.getName());
            log.info(id+": "+mapConfig);
        }
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for(int i=0; i<MapIntegrityThreadCount; i++){
            integrityThreads[i]=new MapIntegrityThread();
            spawner.spawn( integrityThreads[i] );
        }
        for(int i=0; i<stressThreadCount; i++){
            spawner.spawn( new StressThread() );
        }
        spawner.awaitCompletion();
    }

    private class MapIntegrityThread implements Runnable {
        Random random = new Random();
        int nullValueCount=0;
        int sizeErrorCount=0;
        public void run() {
            while (!testContext.isStopped()) {

                int key = random.nextInt(totalIntegritiyKeys);
                byte[] val = integrityMap.get(key);
                int actualSize= integrityMap.size();
                if(doRunAsserts){
                    assertNotNull(id + ": integrityMap=" + integrityMap.getName() + " key " + key + " == null" , val);
                    assertEquals(id + ": integrityMap=" + integrityMap.getName() + " map size ", totalIntegritiyKeys, actualSize);
                }else{
                    if(val==null){
                        nullValueCount++;
                    }
                    if(actualSize != totalIntegritiyKeys){
                        sizeErrorCount++;
                    }
                }
            }
        }
    }

    private class StressThread implements Runnable {
        Random random = new Random();

        public void run() {
            while (!testContext.isStopped()) {
                int key = random.nextInt(totalStressKeys);
                stressMap.put(key, value);
            }
        }
    }

    @Verify(global = false)
    public void verify() throws Exception {
        if(TestUtils.isMemberNode(targetInstance)){
            log.info(id + ": cluster size =" + targetInstance.getCluster().getMembers().size());
        }

        log.info( id + ": integrityMap=" + integrityMap.getName() + " size=" + integrityMap.size());
        int totalErrorCount=0;
        int totalNullValueCount=0;
        for(int i=0; i<integrityThreads.length; i++){
            totalErrorCount += integrityThreads[i].sizeErrorCount;
            totalNullValueCount += integrityThreads[i].nullValueCount;
        }
        log.info( id + ": total integrityMapSizeErrorCount=" + totalErrorCount);
        log.info( id + ": total integrityMapNullValueCount=" + totalNullValueCount);

        assertEquals(id + ": (verify) integrityMap=" + integrityMap.getName() + " map size ", totalIntegritiyKeys, integrityMap.size());
        assertEquals(id + ": (verify) integrityMapSizeErrorCount=", 0, totalErrorCount);
        assertEquals(id + ": (verify) integrityMapNullValueCount=", 0, totalNullValueCount);
    }
}