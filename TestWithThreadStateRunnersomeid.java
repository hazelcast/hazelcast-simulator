import com.hazelcast.simulator.test.*;
import com.hazelcast.simulator.test.annotations.*;
import com.hazelcast.simulator.testcontainer.*;
import com.hazelcast.simulator.worker.*;
import com.hazelcast.simulator.worker.tasks.*;
import com.hazelcast.simulator.worker.metronome.*;
import com.hazelcast.simulator.probes.*;
import java.util.*;
import java.util.concurrent.atomic.*;

public class TestWithThreadStateRunnersomeid extends TimeStepRunner {

    public TestWithThreadStateRunnersomeid(com.hazelcast.simulator.testcontainer.TimeStepRunStrategyIntegrationTest.TestWithThreadState testInstance, TimeStepModel model) {
        super(testInstance, model);
    }

    @Override
    public void timeStepLoop() throws Exception {
        final AtomicLong iterations = this.iterations;
        final TestContextImpl testContext = (TestContextImpl)this.testContext;
        final com.hazelcast.simulator.testcontainer.TimeStepRunStrategyIntegrationTest.TestWithThreadState testInstance = (com.hazelcast.simulator.testcontainer.TimeStepRunStrategyIntegrationTest.TestWithThreadState)this.testInstance;
        final com.hazelcast.simulator.probes.impl.HdrProbe timeStepProbe = (com.hazelcast.simulator.probes.impl.HdrProbe)probeMap.get("timeStep");
        final com.hazelcast.simulator.test.BaseThreadState threadState = (com.hazelcast.simulator.test.BaseThreadState)this.threadState;


        long iteration = 0;
        while (!testContext.isStopped()) {
            long startNanos = System.nanoTime();

            testInstance.timeStep( threadState );
            timeStepProbe.recordValue(System.nanoTime() - startNanos);
            iteration++;
            iterations.lazySet(iteration);
        }
    }
}