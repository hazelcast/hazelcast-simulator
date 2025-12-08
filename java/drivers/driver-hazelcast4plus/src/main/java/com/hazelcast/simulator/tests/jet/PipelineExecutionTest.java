package com.hazelcast.simulator.tests.jet;

import com.hazelcast.config.UserCodeNamespaceConfig;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.utils.CompilationUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * Test to allow benchmarking of Jet pipelines using the simulator as a harness.
 * We are not collecting metrics here, they will be profiled from member diagnostics
 */
public class PipelineExecutionTest
        extends HazelcastTest {

    /**
     * Expected to implement Supplier<Map<JobConfig, Pipeline>>
     */
    public String pipelineSupplierPath = "PipelineSupplier.java";

    /**
     * Supplier class name in the source file
     */
    public String pipelineSupplierClassName = pipelineSupplierPath.replace(".java", "");

    public String ucnResourcesName = "SimulatorJobResources";

    /**
     * How long the worker should wait until the run finishes
     */
    public long waitTimeout = -1;

    @Prepare(global = true)
    public void submitJobs()
            throws Exception {
        Path pipelineSupplierP = Path.of(pipelineSupplierPath);
        if (!Files.exists(pipelineSupplierP)) {
            throw new FileNotFoundException(pipelineSupplierPath);
        }

        Class<?> compilationOutput = CompilationUtils.compile(pipelineSupplierP, pipelineSupplierClassName, new File(""));
        targetInstance.getConfig().getNamespacesConfig()
                      .addNamespaceConfig(new UserCodeNamespaceConfig(ucnResourcesName).addClass(compilationOutput));

        Method getter = compilationOutput.getDeclaredMethod("get");
        Map<JobConfig, Pipeline> pipelines = (Map<JobConfig, Pipeline>) getter.invoke(
                compilationOutput.getConstructor().newInstance());

        for (var entry : pipelines.entrySet()) {
            entry.getKey().setUserCodeNamespace(ucnResourcesName);
            targetInstance.getJet().newJob(entry.getValue(), entry.getKey());
        }
    }

    @Run
    public void waitForFinish() {
        // Simply wait until all jobs to finish
        targetInstance.getJet().getJobs().forEach(Job::join);
    }
}
