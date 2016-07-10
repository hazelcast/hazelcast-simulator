import com.hazelcast.simulator.test.*;
import com.hazelcast.simulator.test.annotations.*;
import com.hazelcast.simulator.worker.*;
import com.hazelcast.simulator.worker.tasks.*;
import com.hazelcast.simulator.worker.metronome.*;
import com.hazelcast.simulator.probes.*;
import java.util.*;
import java.util.concurrent.atomic.*;

public class GeneratedTimeStepRunner_2 extends TimeStepRunner {

    public GeneratedTimeStepRunner_2(Object testInstance, TimeStepModel model) {
        super(testInstance, model);
    }

    @Override
    public void timeStepLoop() throws Exception {
        final AtomicLong iterations = this.iterations;
        final TestContextImpl testContext = (TestContextImpl)this.testContext;
        final com.hazelcast.simulator.worker.TimeStepRunStrategyIntegrationTest.TestWithThreadContext testInstance = (com.hazelcast.simulator.worker.TimeStepRunStrategyIntegrationTest.TestWithThreadContext)this.testInstance;
        final com.hazelcast.simulator.probes.impl.HdrProbe timestepProbe = (com.hazelcast.simulator.probes.impl.HdrProbe)probeMap.get("timestep");
        final com.hazelcast.simulator.test.BaseThreadContext threadContext = (com.hazelcast.simulator.test.BaseThreadContext)this.threadContext;


        long startNanos;
        long iteration = 0;
        while (!testContext.isStopped()) {
            startNanos = System.nanoTime();
            testInstance.timestep( threadContext );
            timestepProbe.recordValue(System.nanoTime() - startNanos);
            iteration++;
            iterations.lazySet(iteration);
        }
    }
}