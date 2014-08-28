package com.hazelcast.stabilizer.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.SerializationServiceBuilder;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.map.helpers.Employee;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;
import com.hazelcast.stabilizer.tests.map.helpers.OppCounterIdxTest;

import java.util.Collection;
import java.util.Random;

import static org.junit.Assert.assertTrue;

public class MapPredicateTest {

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

    public MapPredicateTest(){}

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
            Employee e = new Employee(i);
            map.put(e.getId(), e);
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
        final private SerializationService ss = new SerializationServiceBuilder().build();

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
                        Predicate predicate1 = entryObject.get( "age" ).lessThan(age);
                        Predicate predicate  = entryObject.get( "name" ).equal( name ).and( predicate1 );

                        Collection<Employee> employees = map.values(predicate);
                        for(Employee emp : employees){

                            QueryEntry qe = new QueryEntry(null, ss.toData(emp.getId()), emp.getId(), emp);

                            if( !predicate.apply(qe) ){
                                System.out.println(basename+" ERROR 1: "+emp+" not matching predicate "+predicate);
                            }

                            if( !( emp.getAge() < age && name.equals(emp.getName()) ) ){
                                System.out.println(basename+" ERROR 2: "+emp+" not matching predicate "+predicate);
                            }
                        }
                        counter.predicateBuilderCount++;
                    }

                    else if ( (chance -= sqlString) < 0) {

                        final boolean active = random.nextBoolean();
                        final int age = random.nextInt(Employee.MAX_AGE);

                        final SqlPredicate predicate = new SqlPredicate( "active="+active+" AND age >"+age );
                        Collection<Employee> employees = map.values( predicate );


                        for(Employee emp : employees){

                            QueryEntry qe = new QueryEntry(null, ss.toData(emp.getId()), emp.getId(), emp);
                            if( !predicate.apply(qe) ){
                                System.out.println(basename+" ERROR 1: "+emp+" not matching predicate "+predicate);
                            }

                            if( !(active == emp.isActive() && emp.getAge() > age) ){

                                System.out.println(basename+" ERROR 2: "+emp+" not matching predicate "+predicate);
                            }
                        }
                        counter.sqlStringCount++;
                    }

                    else if ( (chance -= pagePred) < 0) {

                        final double maxSal = random.nextDouble() * Employee.MAX_SALARY;

                        Predicate  pred = Predicates.lessThan("salary", maxSal);
                        PagingPredicate pagingPredicate = new PagingPredicate( pred , 5);
                        Collection<Employee> employees;

                        System.out.println(basename+" start-loop");
                        do{
                            employees = map.values( pagingPredicate );

                            for(Employee emp : employees){
                                assertTrue(emp+" not matching predicate "+pred, emp.getSalary() < maxSal);
                            }

                            pagingPredicate.nextPage();

                            if(testContext.isStopped()){
                                System.out.println(basename+"(stop) res size = "+employees.size());
                            }

                        }while( ! employees.isEmpty() );
                        System.out.println(basename+" end-loop");

                        counter.pagePredCount++;
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

        System.out.println(basename+" "+total+" from "+counters.size());
    }

}