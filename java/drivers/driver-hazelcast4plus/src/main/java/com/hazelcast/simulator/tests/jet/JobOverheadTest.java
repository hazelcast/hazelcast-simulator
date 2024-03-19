package com.hazelcast.simulator.tests.jet;

import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.annotations.TimeStep;

import javax.annotation.Nonnull;

/**
 * Creates a trivial job that does some very simple processing to measure job submission overhead.
 */
public class JobOverheadTest extends HazelcastTest {

    // preallocate to avoid garbage and overhead
    private final JobConfig jobConfig = new JobConfig();
    private final DAG dag = batchJob();

    // properties

    @Nonnull
    private static DAG batchJob() {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(1))
                .writeTo(Sinks.noop());
        return p.toDag();
    }

    @TimeStep
    public void lightJob() throws Exception {
        Job job = targetInstance.getJet().newLightJob(dag, jobConfig);
        job.join();
    }

    @TimeStep
    public void normalJob() {
        Job job = targetInstance.getJet().newJob(dag, jobConfig);
        job.join();
    }
}
