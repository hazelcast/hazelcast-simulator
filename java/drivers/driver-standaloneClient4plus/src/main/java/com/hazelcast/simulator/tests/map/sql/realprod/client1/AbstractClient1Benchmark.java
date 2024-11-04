package com.hazelcast.simulator.tests.map.sql.realprod.client1;

import com.hazelcast.config.IndexType;
import com.hazelcast.map.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import com.hazelcast.sql.SqlService;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import java.text.DateFormatSymbols;
import java.util.ArrayList;
import java.util.List;

public class AbstractClient1Benchmark extends HazelcastTest {
    protected static final long JOB_ID = 2885;

    // properties
    // the number of map entries
    public int entryCount = 1000;

    protected IMap<String, Client1ModelClass2> map;

    public void setUp() {
        this.map = targetInstance.getMap(name);
        map.addIndex(IndexType.HASH, "jobId");
        map.addIndex(IndexType.HASH, "jobId", "monthStr");
        map.addIndex(IndexType.HASH, "jobId", "monthStr", "region");
    }

    public void prepare() {
        Streamer<String, Client1ModelClass2> streamer = StreamerFactory.getInstance(map);

        for (int i = 0; i < entryCount; i++) {
            Client1ModelClass2 class2 = generateRandomMC2(i);
            map.put(class2.getKey(), class2);
            streamer.pushEntry(class2.getKey(), class2);
        }
        streamer.await();
        SqlService sqlService = targetInstance.getSql();

        String query = "CREATE EXTERNAL MAPPING IF NOT EXISTS " + name + " "
                + "EXTERNAL NAME " + name + " "
                + "        TYPE IMap\n"
                + "        OPTIONS (\n"
                + "                'keyFormat' = 'java',\n"
                + "                'keyJavaClass' = 'java.lang.String',\n"
                + "                'valueFormat' = 'java',\n"
                + "                'valueJavaClass' = 'com.hazelcast.simulator.tests.map.sql.realprod.client1.Client1ModelClass2'\n"
                + "        )";

        sqlService.execute(query);
    }

    protected static Client1ModelClass2 generateRandomMC2(int counter) {
        Client1ModelClass2 t = new Client1ModelClass2();

        int monthInt = RandomUtils.nextInt(0, 12);
        String month = getShortMonth(monthInt);
        int year = RandomUtils.nextInt(0, 22) + 2000;

        t.setMonthStr(month + " " + year);
        t.setJobId(JOB_ID);
        t.setRegion(RandomUtils.nextInt(0, 12));
        t.setLong3(RandomUtils.nextLong(0, Long.MAX_VALUE));
        t.setLong4(RandomUtils.nextLong(0, Long.MAX_VALUE));
        t.setString2(RandomStringUtils.random(3, "abc"));
        t.setInt2(RandomUtils.nextInt(0, 1_000_000));

        t.setString1(RandomStringUtils.random(3, "def"));
        t.setInt1(RandomUtils.nextInt(0, 1000));

        return t;
    }

    protected static String getRandomMonth() {
        String month = getShortMonth(RandomUtils.nextInt(0, 12));
        int year = RandomUtils.nextInt(0, 22) + 2000;
        return month + " " + year;
    }

    private static String getShortMonth(int monthIndex) {
        return new DateFormatSymbols().getShortMonths()[monthIndex];
    }

    protected static List<Integer> getRandomRegionList() {
        int size = RandomUtils.nextInt(0, 5) + 1;
        List<Integer> reignIdList = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            reignIdList.add(RandomUtils.nextInt(0, 12));
        }

        return reignIdList;
    }

    public void tearDown() {
        map.destroy();
    }
}
