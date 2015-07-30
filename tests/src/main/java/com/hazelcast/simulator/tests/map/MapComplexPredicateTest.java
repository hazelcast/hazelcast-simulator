package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.tests.map.helpers.ComplexDomainObject;
import com.hazelcast.simulator.utils.ThrottlingLogger;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;

import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.logging.Level.INFO;

public class MapComplexPredicateTest {
    private final static ILogger log = Logger.getLogger(MapComplexPredicateTest.class);
    private static final ThrottlingLogger tl = ThrottlingLogger.newLogger(log, 5000);

    public String basename = MapComplexPredicateTest.class.getSimpleName();
    public int mapSize = 1000000;
    public String query = "disabled_for_upload = false " + //Done
            " AND ( media_id != -1 AND inactive = false AND quality_check_state = 12 AND proxy_creation_state = 14 " + //Done
            " AND ( ( media_content_approval_instruction != 3 AND content_approval_media_state != 19 AND content_approval_media_state != 17) " + // Done
            " OR ( media_clock_approval_instruction != 31 AND clock_approval_media_state != 41) OR ( media_instruction_approval_instruction != 51 " + //Done
            " AND ( num_instruction_approval_not_performed > 0 OR num_instruction_rejected > 0)) OR ( media_class_approval_instruction != 41 " + //Done
            " AND class_approval_media_state != 71) OR ( media_house_nr = '' AND media_house_nr_instruction = 10)) AND ( content_approval_media_state != 18 " + //Done
            " AND clock_approval_media_state != 42 AND class_approval_media_state != 72 AND num_instruction_rejected = 0 AND qc_approval_media_state != 22 " + //Done
            " AND approval_advert_state != 11) AND ( aired = false) AND ( approval_advert_state = 10 OR approval_advert_state = 9) AND ( NOT ( time_delivered <= 0 " + //Done
            " AND stopped = false AND inactive = false AND quality_check_state = 12 AND proxy_creation_state = 14 AND ( content_approval_media_state = 19 " + //Done
            " OR content_approval_media_state = 17 OR media_content_approval_instruction != 1) AND ( class_approval_media_state = 71 OR media_class_approval_instruction != 40) " + //Done
            "AND ( clock_approval_media_state = 41 OR media_clock_approval_instruction != 30) " + //Done
            "AND ( media_instruction_approval_instruction != 50 OR ( num_instruction_approval_not_performed = 0 AND num_instruction_rejected = 0)) " + //Done
            "AND ( approval_advert_state = 10 OR approval_advert_state = 9) AND ( media_house_nr != '' OR media_house_nr_instruction = 11))) " + //Done
            "AND ( media_id > 60000000))";


    private Random random = new Random();
    private IMap<String, ComplexDomainObject> map;

    @Setup
    public void setUp(TestContext testContext) throws Exception {
        map = testContext.getTargetInstance().getMap(basename + "-" + testContext.getTestId());
    }

    @Warmup(global = true)
    public void warmUp() {
        Streamer<String, ComplexDomainObject> streamer = StreamerFactory.getInstance(map);
        for (int i = 0; i < mapSize; i++) {
            ComplexDomainObject value = createQuickSearchObject(i);
            String key = value.getQuickSearchKey();
            streamer.pushEntry(key, value);
        }
        streamer.await();
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private int randomUpTo14() {
        return random.nextInt(14);
    }

    private long randomLong() {
        return random.nextLong();
    }

    public ComplexDomainObject createQuickSearchObject(int id) {
        return new ComplexDomainObject(id, randomLong() * 2,
                "l-name" + randomUpTo14(),
                randomLong() * 3,
                "d-name" + randomUpTo14(),
                randomLong() * 4,
                "b-name" + randomUpTo14(),
                randomLong() * 5,
                "m-name" + randomUpTo14(),
                random.nextInt(4),//media_content_approval_instruction
                randomUpTo14(),
                random.nextInt(32),//media_clock_approval_instruction
                randomUpTo14() * 3,
                random.nextInt(52),//media_instruction_approval_instruction
                10,//media_house_number_instruction
                true,
                randomUpTo14() * 3,
                randomLong() * 3,
                randomLong() * 2,
                randomLong() * 2,
                randomLong() * 2,
                randomLong() * 1,
                "copy_code" + randomUpTo14(),
                random.nextBoolean(),//aired
                randomLong() * 1,
                randomUpTo14(),
                randomLong() * 3,
                randomUpTo14() * 3,
                "title" + randomUpTo14(),
                random.nextBoolean(),
                random.nextBoolean(),
                randomLong() * 2,
                randomLong() * 2,
                randomLong() * 3,
                randomLong() * 3,
                randomUpTo14() * 3,
                randomUpTo14() * 3,
                random.nextBoolean(),
                randomLong() * 3,
                random.nextBoolean(),
                "media_agency_planning" + randomUpTo14(),
                "media_agency_buying" + randomUpTo14(),
                "creative_agency" + randomUpTo14(),
                "production_agency" + randomUpTo14(),
                "post_production_agency" + randomUpTo14(),
                "other_agency" + randomUpTo14(),
                random.nextBoolean(),
                "media_house_nr" + randomUpTo14(),
                "b_p_code" +randomUpTo14(),
                random.nextBoolean(),
                random.nextBoolean(),
                randomLong() * 3,
                randomLong() * 3,
                randomLong() * 2,
                random.nextBoolean(),//inactive
                randomLong() * 2,
                randomUpTo14() * 3,
                randomLong() * 2,
                randomLong() * 3,
                randomLong() * 3,
                randomLong() * 2,
                10,//approval_advert_state
                12,//quality_check_state
                14,
                randomLong() * 3,
                random.nextBoolean(),
                randomLong() * 3,
                randomUpTo14() * 2,
                randomLong() * 2,
                randomLong() * 3,
                randomLong() * 3,
                randomLong() * 2,
                randomLong(),
                randomLong() * 2,
                randomLong() * 3,
                randomLong() * 2,
                random.nextInt(20),//content_approval_media_state
                randomLong() * 2,
                randomLong() * 3,
                randomUpTo14() * 3,
                randomLong() * 3,
                randomLong() * 3,
                randomUpTo14() * 2,
                random.nextInt(42),//clock_approval_media_state
                randomLong() * 3,
                randomUpTo14() * 3,
                randomUpTo14() * 2,
                random.nextInt(4), //num_instruction_rejected
                random.nextInt(2),//num_instruction_approval_not_performed
                randomLong() * 3,
                randomUpTo14() * 2,
                random.nextBoolean(),
                randomUpTo14() * 3);
    }

    private class Worker extends AbstractMonotonicWorker {
        private Predicate<String, ComplexDomainObject> predicate;

        @Override
        protected void beforeRun() {
            predicate = new SqlPredicate(query);
        }

        @Override
        protected void timeStep() {
            long startTime = System.nanoTime();
            Set<Map.Entry<String, ComplexDomainObject>> entries = map.entrySet(predicate);
            long durationNanos = System.nanoTime() - startTime;
            long durationMillis = TimeUnit.NANOSECONDS.toMillis(durationNanos);

            tl.log(INFO, "Query Evaluation Took " + durationMillis + "ms. Size of the result size: " + entries.size());
        }
    }
}
