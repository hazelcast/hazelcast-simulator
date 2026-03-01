package com.hazelcast.simulator.tests.jet;

import com.hazelcast.config.UserCodeNamespaceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.utils.CompilationUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * Test to allow benchmarking of Jet pipelines using the simulator as a harness.
 * We are not collecting metrics here, they will be profiled from member diagnostics.
 */
public class PipelineExecutionTest
        extends HazelcastTest {

    private static final String UPLOAD_DIR = "upload";
    private static final String COMPILATION_OUTPUT_DIR = "compilation-output";

    /**
     * Name of file contained in the "upload" directory which will be compiled and
     * used to get the Pipelines we want to run. It is expected to be a standard Java
     * class which implements Supplier<Map<JobConfig, Pipeline>>.
     */
    public String pipelineSupplierFileName;

    /**
     * Name used for UCN which we will upload compiled resources to
     */
    public String ucnResourcesName = "SimulatorJobResources";

    // The pipeline context is created dynamically by first compiling a .java file uploaded to this
    // loadgenerator which is expected to be a class implementing Supplier<Map<JobConfig, Pipeline>>.
    // The supplier is then called to get the job definitions which are then submitted to the cluster.
    private Object pipelineContext;

    private List<Job> submittedJobs;

    @SuppressWarnings("unchecked")
    @Prepare(global = true)
    public void submitJobs()
            throws Exception {
        if (pipelineSupplierFileName == null) {
            throw new IllegalStateException("Must set \"pipelineSupplierFileName\" attribute in tests.yaml");
        }

        Path pipelineSupplierP = Path.of(UPLOAD_DIR + "/" + pipelineSupplierFileName);
        if (!Files.exists(pipelineSupplierP)) {
            throw new FileNotFoundException(pipelineSupplierFileName);
        }

        File compilationOutputDir = new File(COMPILATION_OUTPUT_DIR);
        if (!compilationOutputDir.mkdir()) {
            throw new IllegalStateException("Failed to create directory at " + compilationOutputDir);
        }

        Class<?> compilationOutput = CompilationUtils.compile(pipelineSupplierP, getPipelineSupplierClassName(),
                compilationOutputDir);
        targetInstance.getConfig().getNamespacesConfig()
                      .addNamespaceConfig(new UserCodeNamespaceConfig(ucnResourcesName).addClass(compilationOutput));

        pipelineContext = compilationOutput.getConstructor().newInstance();

        if (pipelineContext instanceof Supplier<?> pipelineSupplier) {
            Map<JobConfig, Pipeline> pipelines = (Map<JobConfig, Pipeline>) pipelineSupplier.get();
            submittedJobs = new ArrayList<>();
            for (var entry : pipelines.entrySet()) {
                entry.getKey().setUserCodeNamespace(ucnResourcesName);
                submittedJobs.add(targetInstance.getJet().newJob(entry.getValue(), entry.getKey()));
            }
        } else {
            throw new RuntimeException("Injected class must implement Supplier<Map<JobConfig, Pipeline>>");
        }
    }

    private String getPipelineSupplierClassName() {
        return pipelineSupplierFileName.replace(".java", "");
    }

    @SuppressWarnings("unchecked")
    @Run
    public void waitForFinish() {
        if (pipelineContext instanceof BiConsumer<?, ?> waitForStop) {
            logger.info("Using injectedContext as wait condition");
            ((BiConsumer<HazelcastInstance, List<Job>>) waitForStop).accept(targetInstance, submittedJobs);
        } else {
            logger.info("Waiting for all jobs to complete");
            joinAllJobs();
        }

        // Cancel any jobs still running
        for (Job job : submittedJobs) {
            try {
                job.cancel();
            } catch (Exception e) {
                logger.info("Cancellation of job " + job.getName() + " failed", e);
            }
        }
    }

    private void joinAllJobs() {
        submittedJobs.forEach(Job::join);
    }
}
