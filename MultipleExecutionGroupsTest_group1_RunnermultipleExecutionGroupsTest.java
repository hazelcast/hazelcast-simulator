import com.hazelcast.simulator.test.*;
import com.hazelcast.simulator.test.annotations.*;
import com.hazelcast.simulator.worker.testcontainer.*;
import com.hazelcast.simulator.worker.*;
import com.hazelcast.simulator.worker.tasks.*;
import com.hazelcast.simulator.worker.metronome.*;
import com.hazelcast.simulator.probes.*;
import java.util.*;
import java.util.concurrent.atomic.*;

public class MultipleExecutionGroupsTest_group1_RunnermultipleExecutionGroupsTest extends TimeStepRunner {

    public MultipleExecutionGroupsTest_group1_RunnermultipleExecutionGroupsTest(com.hazelcast.simulator.worker.testcontainer.TestContainer_TimeStepMultipleExecutionGroupsTest.MultipleExecutionGroupsTest testInstance, TimeStepModel model, String executionGroup) {
        super(testInstance, model, executionGroup);
    }

    @Override
    public void timeStepLoop() throws Exception {
        final AtomicLong iterations = this.iterations;
        final TestContextImpl testContext = (TestContextImpl)this.testContext;
        final com.hazelcast.simulator.worker.testcontainer.TestContainer_TimeStepMultipleExecutionGroupsTest.MultipleExecutionGroupsTest testInstance = (com.hazelcast.simulator.worker.testcontainer.TestContainer_TimeStepMultipleExecutionGroupsTest.MultipleExecutionGroupsTest)this.testInstance;
        final com.hazelcast.simulator.probes.impl.HdrProbe group1TimeStepProbe = (com.hazelcast.simulator.probes.impl.HdrProbe)probeMap.get("group1TimeStep");


        long iteration = 0;
        while (!testContext.isStopped()) {
            long startNanos = System.nanoTime();

            testInstance.group1TimeStep( );
            group1TimeStepProbe.recordValue(System.nanoTime() - startNanos);
            iteration++;
            iterations.lazySet(iteration);
        }
    }
}