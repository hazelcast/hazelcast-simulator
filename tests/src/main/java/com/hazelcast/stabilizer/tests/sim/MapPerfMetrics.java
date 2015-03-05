package com.hazelcast.stabilizer.tests.sim;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import java.io.File;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.test.TestContext;
import com.hazelcast.stabilizer.test.annotations.Run;
import com.hazelcast.stabilizer.test.annotations.Setup;
import com.hazelcast.stabilizer.test.annotations.Warmup;
import com.hazelcast.stabilizer.test.utils.ThreadSpawner;

public class MapPerfMetrics {

    private final static ILogger log = Logger.getLogger(LoadMaps.class);

    public String cvsDirPath = "~/";

    public String baseMapName = "map";

    public int threadCount = 8;
    public int totalMaps = 10;
    public int totalKeys = 10;
    public int valueByteArraySize = 1000;

    public double putProb= 1.0;
    public double getProb= 0.0;
    public double setProb= 0.0;

    public boolean fillMaps=false;
    public int reportSecondsInterval=5;

    private File cvsDir;
    private byte[] value;
    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private String id;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        id=testContext.getTestId();

        value = new byte[valueByteArraySize];
        Random random = new Random();
        random.nextBytes(value);

        cvsDir = new File(cvsDirPath);
    }

    @Warmup(global = true)
    public void warmup() throws Exception {
        if(fillMaps){
            for(int m =0; m<totalMaps; m++){
                IMap map = targetInstance.getMap(baseMapName+m);
                for (int k = 0; k < totalKeys; k++) {
                    map.put(k, value);
                }
            }
        }
        printMapInfo();
    }

    public void printMapInfo(){
        for(int i=0; i< totalMaps; i++){
            IMap map = targetInstance.getMap(baseMapName+i);
            log.info(id + ": mapName=" + map.getName() + " size=" + map.size());
        }
    }

    @Run
    public void run() {
        MetricRegistry metrics = new MetricRegistry();

        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new LoadProducer(metrics));
        }

        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics).build();
        CsvReporter csvReporter = CsvReporter.forRegistry(metrics).build(cvsDir);

        csvReporter.start(reportSecondsInterval, TimeUnit.SECONDS);

        spawner.awaitCompletion();

        reporter.report();
    }

    private class LoadProducer implements Runnable {

        private Random random = new Random();
        private MetricRegistry metrics;
        private Timer getTimer;
        private Timer putTimer;
        private Timer setTimer;

        public LoadProducer(MetricRegistry metrics){
            this.metrics = metrics;
            getTimer = metrics.timer("getTimer");
            putTimer = metrics.timer("putTimer");
            setTimer = metrics.timer("setTimer");
        }

        public void run() {
            while (!testContext.isStopped()) {

                IMap map = targetInstance.getMap(baseMapName+random.nextInt(totalMaps));

                double chance = random.nextDouble();

                if((chance -= putProb)<=0) {
                    Timer.Context context =  putTimer.time();
                    Object o = map.put(random.nextInt(totalKeys), value);
                    context.stop();

                }else if((chance-=getProb)<=0){
                    Timer.Context context =  getTimer.time();
                    Object o = map.get(random.nextInt(totalKeys));
                    context.stop();

                }else{
                    Timer.Context context =  setTimer.time();
                    map.set(random.nextInt(totalKeys), value);
                    context.stop();
                }
            }
        }
    }
}