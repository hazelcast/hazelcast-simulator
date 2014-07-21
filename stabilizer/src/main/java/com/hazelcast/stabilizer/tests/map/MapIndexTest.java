package com.hazelcast.stabilizer.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.TestRunner;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.map.helpers.Employee;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;
import com.hazelcast.stabilizer.tests.map.helpers.OppCounterIdxTest;

import java.util.Collection;
import java.util.Random;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class MapIndexTest {

    public String basename = this.getClass().getName();
    public int threadCount = 3;
    public int keyCount = 100;

    public double predicateBuilder=0.2;
    public double sqlString=0.2;
    public double pagePred=0.2;
    public double updateEmploye=0.3;
    public double destroyProb = 0.1;


    private TestContext testContext;
    private HazelcastInstance targetInstance;

    public MapIndexTest(){}

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
    }

    @Warmup(global = true)
    public void globalWarmup() {
        initMap();
    }

    private void initMap(){
        IMap map = targetInstance.getMap(basename);

        for(int i=0; i<keyCount; i++){
            map.put(i, new Employee(i));
        }

        long free = Runtime.getRuntime().freeMemory();
        long total =  Runtime.getRuntime().totalMemory();
        long used = total - free;
        System.out.println("used = "+humanReadableByteCount(used, true));

        map.addIndex( "id", true );
        map.addIndex( "name", true );
        map.addIndex( "age", true );
        map.addIndex( "salary", true );
        map.addIndex( "active", false );

        free = Runtime.getRuntime().freeMemory();
        total =  Runtime.getRuntime().totalMemory();
        used = total - free;
        System.out.println("used = "+humanReadableByteCount(used, true));
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
        private final Random random = new Random();
        private OppCounterIdxTest counter = new OppCounterIdxTest();

        @Override
        public void run() {
            while (!testContext.isStopped()) {
                try{
                    final IMap<Integer, Employee> map = targetInstance.getMap(basename);
                    double chance = random.nextDouble();

                    if ( (chance -= predicateBuilder) < 0) {

                        final int age = random.nextInt(Employee.MAX_AGE);
                        final String name = Employee.names[random.nextInt(Employee.names.length)];

                        EntryObject entryObject = new PredicateBuilder().getEntryObject();
                        Predicate agePredicate = entryObject.get( "age" ).lessThan(age);
                        Predicate predicate = entryObject.get( "name" ).equal( name ).and( agePredicate );
                        Collection<Employee> employees = map.values(predicate);

                        counter.predicateBuilderCount++;

                        for(Employee emp : employees){
                            assertTrue( emp.getAge() < age );
                            assertEquals( name, emp.getName());
                        }
                    }

                    else if ( (chance -= sqlString) < 0) {

                        final boolean avtive = random.nextBoolean();
                        final int age = random.nextInt(Employee.MAX_AGE);
                        Collection<Employee> employees = map.values( new SqlPredicate( "active="+avtive+" AND age >"+age ) );

                        counter.sqlStringCount++;

                        for(Employee emp : employees){
                            assertTrue( avtive == emp.isActive());
                            assertTrue(emp.getAge() > age);
                        }
                    }

                    else if ( (chance -= pagePred) < 0) {


                        final int maxAge = random.nextInt(Employee.MAX_AGE);
                        final double maxSal = random.nextDouble() * Employee.MAX_SALARY;

                        Predicate  betweenAge = Predicates.between("age", maxAge-10, maxAge);
                        Predicate  betweenSlayer = Predicates.between("salary", maxSal-100.0, maxSal);



                        PagingPredicate pagingPredicate = new PagingPredicate( Predicates.and(betweenAge, betweenSlayer), 5);
                        Collection<Employee> employees = map.values( pagingPredicate );

                        counter.pagePredCount++;

                        for(Employee emp : employees){
                            assertTrue( emp.getAge() > maxAge-10 && emp.getAge() < maxAge);
                            assertTrue( emp.getSalary() > maxSal-100.0 && emp.getSalary() < maxSal);
                        }

                    }

                    else if ( (chance -= updateEmploye) < 0 ){

                        int key = random.nextInt(keyCount);
                        Employee e = map.get(key);
                        if(e!=null){
                            e.setInfo();
                            map.put(key, e);
                            counter.updateEmployeCount++;
                        }
                    }

                    else if ( (chance -= destroyProb) < 0 ){
                        map.destroy();
                        initMap();

                        counter.destroyCount++;
                    }
                }catch(DistributedObjectDestroyedException e){}
            }
            targetInstance.getList(basename+"report").add(counter);
        }
    }

    @Verify(global = true)
    public void globalVerify() throws Exception {

        IList<OppCounterIdxTest> counters = targetInstance.getList(basename+"report");

        OppCounterIdxTest total = new OppCounterIdxTest();
        for(OppCounterIdxTest c : counters){
            total.add(c);
        }

        System.out.println(basename+" "+total+" form "+counters.size());
    }

    @Verify(global = false)
    public void verify() throws Exception {

        try{
            final IMap map = targetInstance.getMap(basename);

            System.out.println(basename+ ": map size  =" + map.size() );

            long free = Runtime.getRuntime().freeMemory();
            long total =  Runtime.getRuntime().totalMemory();
            long used = total - free;
            System.out.println("used = "+humanReadableByteCount(used, true));
        }catch(UnsupportedOperationException e){}
    }

    public static void main(String[] args) throws Throwable {
        new TestRunner(new MapIndexTest()).run();
    }

    public static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp-1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }
}