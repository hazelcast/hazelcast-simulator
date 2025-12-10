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

    private static final int MILLIS_IN_SECOND = 1000;
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

    /**
     * Wait after all jobs have completed for this amount of time, can be used to ensure
     * final metric values output in diagnostics.
     */
    public long waitAfterJobCompletionSeconds = 0L;

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
        compilationOutputDir.mkdir();

        Class<?> compilationOutput = CompilationUtils.compile(pipelineSupplierP, getPipelineSupplierClassName(),
                compilationOutputDir);
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

    private String getPipelineSupplierClassName() {
        return pipelineSupplierFileName.replace(".java", "");
    }

    @Run
    public void waitForFinish() {
        // Simply wait until all jobs to finish
        logger.info("Joining jobs");
        targetInstance.getJet().getJobs().forEach(Job::join);
        if (waitAfterJobCompletionSeconds > 0) {
            logger.info("Waiting after jobs for {} seconds", waitAfterJobCompletionSeconds);
            try {
                Thread.sleep(waitAfterJobCompletionSeconds * MILLIS_IN_SECOND);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
