package com.hazelcast.simulator.tests.jet;

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Run;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * Test to allow benchmarking of Jet pipelines using the simulator as a harness.
 * We are not collecting metrics here, they will be profiled from member diagnostics
 */
public class PipelineExecutionTest
        extends HazelcastTest {

    private final JavaCompiler javaCompiler = ToolProvider.getSystemJavaCompiler();

    /**
     * Expected to implement Supplier<Map<JobConfig, Pipeline>>
     */
    public String pipelineSupplierPath = "PipelineSupplier.java";

    /**
     * How long the worker should wait until the run finishes
     */
    public long waitTimeout = -1;

    private Map<JobConfig, Pipeline> pipelines;

    @Prepare
    public void loadPipelines() throws FileNotFoundException {
        Path pipelineSupplierP = Path.of(pipelineSupplierPath);
        if (!Files.exists(pipelineSupplierP)) {
            throw new FileNotFoundException(pipelineSupplierPath);
        }


        //        Path pipelineSupplierJavaPath = Path.of(pipelineSupplierFileName + )
        // Dynamically compile fragment if .java file
        // Load the compiled class and invoke the get() method
    }

    @Prepare(global = true)
    public void submitJob() {
        // Idea is a code fragment has been supplied which we dynamically compile
        // and load before invoking to get the pipeline we actually want to submit
        // Then we submit it here.
    }

    @Run
    public void waitForFinish() {
        // Simply wait until all jobs to finish
    }
}
