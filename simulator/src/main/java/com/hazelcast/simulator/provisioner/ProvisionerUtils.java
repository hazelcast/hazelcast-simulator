package com.hazelcast.simulator.provisioner;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.provisioner.git.BuildSupport;
import com.hazelcast.simulator.provisioner.git.GitSupport;
import com.hazelcast.simulator.provisioner.git.HazelcastJARFinder;
import com.hazelcast.simulator.utils.CommandLineExitException;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import static com.hazelcast.simulator.utils.CloudProviderUtils.isStatic;
import static java.lang.String.format;

final class ProvisionerUtils {

    static final String INIT_SH_SCRIPT_NAME = "init.sh";

    private ProvisionerUtils() {
    }

    static File getInitScriptFile(String simulatorHome) {
        File initScript = new File(INIT_SH_SCRIPT_NAME);
        if (!initScript.exists()) {
            initScript = new File(simulatorHome + "/conf/" + INIT_SH_SCRIPT_NAME);
        }
        if (!initScript.exists()) {
            throw new CommandLineExitException(format("Could not find %s: %s", INIT_SH_SCRIPT_NAME, initScript));
        }
        return initScript;
    }

    static HazelcastJars getHazelcastJars(Bash bash, SimulatorProperties properties) {
        return new HazelcastJars(bash, createGitSupport(bash, properties), properties.getHazelcastVersionSpec());
    }

    private static GitSupport createGitSupport(Bash bash, SimulatorProperties properties) {
        String mvnExec = properties.get("MVN_EXECUTABLE");
        BuildSupport buildSupport = new BuildSupport(bash, new HazelcastJARFinder(), mvnExec);
        String gitBuildDirectory = properties.get("GIT_BUILD_DIR");
        String customGitRepositories = properties.get("GIT_CUSTOM_REPOSITORIES");

        return new GitSupport(buildSupport, customGitRepositories, gitBuildDirectory);
    }

    static void ensureNotStaticCloudProvider(SimulatorProperties properties, String action) {
        if (isStatic(properties.get("CLOUD_PROVIDER"))) {
            throw new CommandLineExitException(format("Cannot execute '%s' in static setup", action));
        }
    }

    static int[] calcBatches(SimulatorProperties properties, int size) {
        List<Integer> batches = new LinkedList<Integer>();
        int batchSize = Integer.parseInt(properties.get("CLOUD_BATCH_SIZE"));
        while (size > 0) {
            int currentBatchSize = (size >= batchSize ? batchSize : size);
            batches.add(currentBatchSize);
            size -= currentBatchSize;
        }

        int[] result = new int[batches.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = batches.get(i);
        }
        return result;
    }
}
