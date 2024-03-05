/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.coordinator.registry.Registry;
import com.hazelcast.simulator.coordinator.registry.WorkerQuery;
import com.hazelcast.simulator.coordinator.tasks.AgentsClearTask;
import com.hazelcast.simulator.coordinator.tasks.AgentsDownloadTask;
import com.hazelcast.simulator.utils.CommandLineExitException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.common.GitInfo.getBuildTime;
import static com.hazelcast.simulator.common.GitInfo.getCommitIdAbbrev;
import static com.hazelcast.simulator.utils.CliUtils.initOptionsWithHelp;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.SimulatorUtils.loadSimulatorProperties;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

@SuppressWarnings("FieldCanBeLocal")
final class CoordinatorCli {
    private static final Logger LOGGER = LogManager.getLogger(CoordinatorCli.class);

    CoordinatorRunMonolith runMonolith;
    Coordinator coordinator;
    TestSuite testSuite;
    CoordinatorParameters coordinatorParameters;
    Registry registry;
    SimulatorProperties properties;
    DeploymentPlan deploymentPlan;

    private final OptionParser parser = new OptionParser();

    private final OptionSpec<Integer> workerVmStartupDelayMsSpec = parser.accepts("workerVmStartupDelayMs",
                    "Amount of time in milliseconds to wait between starting up the next worker. This is useful to prevent"
                            + "duplicate connection issues.")
            .withRequiredArg().ofType(Integer.class).defaultsTo(0);
    
    private final OptionSpec<TargetType> targetTypeSpec = parser.accepts("targetType",
                    format("Defines the type of Workers which execute the RUN phase."
                            + " The type PREFER_CLIENT selects client Workers if they are available, member Workers otherwise."
                            + " List of allowed types: %s", TargetType.getIdsAsString()))
            .withRequiredArg().ofType(TargetType.class).defaultsTo(TargetType.PREFER_CLIENT);

    private final OptionSpec<String> clientTypeSpec = parser.accepts("clientType",
                    "Defines the type of client e.g javaclient, litemember, etc.")
            .withRequiredArg().ofType(String.class).defaultsTo("javaclient");

    private final OptionSpec<Integer> targetCountSpec = parser.accepts("targetCount",
                    "Defines the number of Workers which execute the RUN phase. The value 0 selects all Workers.")
            .withRequiredArg().ofType(Integer.class).defaultsTo(0);

    private final OptionSpec<String> runPathSpec = parser.accepts("runPath", "The path to store the results")
            .withRequiredArg().ofType(String.class)
            .defaultsTo("runs/" + new SimpleDateFormat("yyyy-mm-dd_hh:mm:ss").format(Calendar.getInstance().getTime()));

    private final OptionSpec<TestPhase> syncToTestPhaseSpec = parser.accepts("syncToTestPhase",
                    format("Defines the last TestPhase which is synchronized between all parallel running tests."
                            + " Use --syncToTestPhase %s to synchronize all test phases."
                            + " List of defined test phases: %s", TestPhase.getLastTestPhase(), TestPhase.getIdsAsString()))
            .withRequiredArg().ofType(TestPhase.class).defaultsTo(TestPhase.getLastTestPhase());

    private final OptionSpec skipDownloadSpec = parser.accepts("skipDownload",
            "Prevents downloading of the created worker artifacts.");

    private final OptionSpec downloadSpec = parser.accepts("download",
            "Downloads all the session directories and applies postprocessing. "
                    + "If this option is set, no other tasks are executed. "
                    + "If '--runPath' is set, only that session is downloaded.");

    private final OptionSpec cleanSpec = parser.accepts("clean",
            "Cleans the remote Worker directories on the provisioned machines. "
                    + "If this option is set, no other tasks are executed");

    private final OptionSpec<String> paramSpec = parser.accepts("param",
                    "A key=value parameter.")
            .withRequiredArg().ofType(String.class);


    private final OptionSet options;

    CoordinatorCli(String[] args) {
        this.options = initOptionsWithHelp(parser, args);

        this.properties = loadSimulatorProperties();

        // Add all the params to the properties
        List<String> propertyList = paramSpec.values(options);
        for (String property : propertyList) {
            int indexOf = property.indexOf("=");
            if (indexOf == -1) {
                throw new CommandLineExitException("Invalid property '" + property + "', should have format key=value");
            }
            String key = property.substring(0, indexOf);
            String value = property.substring(indexOf + 1);
            properties.set(key, value);

            if(!value.contains("\n")) {
                LOGGER.info(key + "=" + value);
            }
        }



//        if (!"fake".equals(properties.get("driver"))) {
//            properties.set("driver", options.valueOf(driverSpec));
//        }

        this.registry = Registry.loadInventoryYaml(
                locateInventoryFile(),
                properties.get("loadgenerator_hosts"),
                properties.get("node_hosts"));

        if (!(options.has(downloadSpec) || options.has(cleanSpec))) {
            this.coordinatorParameters = loadCoordinatorParameters();
            this.coordinator = new Coordinator(registry, coordinatorParameters);
            this.testSuite = loadTestSuite();

            if (testSuite == null) {
                throw new CommandLineExitException("Can't run without a testSuite");
            } else {
                this.deploymentPlan = newDeploymentPlan();
                this.runMonolith = new CoordinatorRunMonolith(coordinator, coordinatorParameters);
            }
        }
    }

    void run() throws Exception {
        if (options.has(downloadSpec)) {
            String runPath = options.has(runPathSpec) ? options.valueOf(runPathSpec) : "*";
            new AgentsDownloadTask(
                    registry,
                    properties.asMap(),
                    new File(runPath),
                    CoordinatorParameters.toSHA1(runPath)).run();
        } else if (options.has(cleanSpec)) {
            new AgentsClearTask(registry).run();
        } else {
            coordinator.start();
            runMonolith.init(deploymentPlan);
            boolean success = runMonolith.run(testSuite);
            System.exit(success ? 0 : 1);
        }
    }

    private CoordinatorParameters loadCoordinatorParameters() {
        return new CoordinatorParameters()
                .setSimulatorProperties(properties)
                .setLastTestPhaseToSync(options.valueOf(syncToTestPhaseSpec))
                .setSkipDownload(options.has(skipDownloadSpec))
                .setWorkerVmStartupDelayMs(options.valueOf(workerVmStartupDelayMsSpec))
                .setRunPath(options.valueOf(runPathSpec));
    }

    private TestSuite loadTestSuite() {
        TestSuite testSuite = loadRawTestSuite();
        if (testSuite == null) {
            return null;
        }

        WorkerQuery workerQuery = new WorkerQuery()
                .setTargetType(options.valueOf(targetTypeSpec));

        int targetCount = options.valueOf(targetCountSpec);
        if (targetCount > 0) {
            workerQuery.setMaxCount(targetCount);
        }

        int durationSeconds = getDurationSeconds(properties.get("duration"));
        testSuite.setDurationSeconds(durationSeconds)
                .setFailFast(properties.getBoolean("fail_fast"))
                .setVerifyEnabled(properties.getBoolean("verify_enabled"))
                .setParallel(properties.getBoolean("parallel"))
                .setWorkerQuery(workerQuery);

        // if the coordinator is not monitoring performance, we don't care for measuring latencies
        if (coordinatorParameters.getSimulatorProperties().getInt("performance_monitor_interval_seconds") == 0) {
            for (TestCase testCase : testSuite.getTestCaseList()) {
                testCase.setProperty("measureLatency", "false");
            }
        }

        return testSuite;
    }

    private TestSuite loadRawTestSuite() {
        String content;
        List testsuiteFiles = options.nonOptionArguments();
        File defaultTestProperties = new File("test.properties");
        if (testsuiteFiles.size() > 1) {
            throw new CommandLineExitException(format("Too many TestSuite files specified: %s", testsuiteFiles));
        } else if (testsuiteFiles.size() == 1) {
            content = (String) testsuiteFiles.get(0);
        } else if (defaultTestProperties.exists()) {
            content = defaultTestProperties.getPath();
        } else {
            return null;
        }

        TestSuite testSuite;
        File testSuiteFile = new File(content);
        if (testSuiteFile.exists()) {
            LOGGER.info("Loading TestSuite file: " + testSuiteFile.getAbsolutePath());
            testSuite = new TestSuite(testSuiteFile);
        } else if (!content.endsWith(".properties")) {
            testSuite = new TestSuite(content);
        } else {
            throw new CommandLineExitException(format("TestSuite file '%s' not found", testSuiteFile));
        }
        return testSuite;
    }

    public static int getDurationSeconds(String durationString) {
        int durationSec;
        try {
            if (durationString.endsWith("s")) {
                durationSec = parseDurationWithoutLastChar(SECONDS, durationString);
            } else if (durationString.endsWith("m")) {
                durationSec = parseDurationWithoutLastChar(MINUTES, durationString);
            } else if (durationString.endsWith("h")) {
                durationSec = parseDurationWithoutLastChar(HOURS, durationString);
            } else if (durationString.endsWith("d")) {
                durationSec = parseDurationWithoutLastChar(DAYS, durationString);
            } else {
                durationSec = Integer.parseInt(durationString);
            }
        } catch (NumberFormatException e) {
            throw new CommandLineExitException(format("Failed to parse duration '%s'", durationString), e);
        }

        if (durationSec < 0) {
            throw new CommandLineExitException("duration must be a positive number, but was: " + durationSec);
        }
        return durationSec;
    }

    public static int parseDurationWithoutLastChar(TimeUnit timeUnit, String value) {
        String sub = value.substring(0, value.length() - 1);
        return (int) timeUnit.toSeconds(Integer.parseInt(sub));
    }

    private DeploymentPlan newDeploymentPlan() {
        String workerType = options.valueOf(clientTypeSpec);
        if ("member".equals(workerType)) {
            throw new CommandLineExitException("client workerType can't be [member]");
        }

        int nodeCount = properties.getInt("node_count");
        if (nodeCount == -1) {
            nodeCount = registry.agentCount();
        } else if (nodeCount < -1) {
            throw new CommandLineExitException("node_count must be a equal or larger than -1");
        }

        int loadGeneratorCount = properties.getInt("loadgenerator_count");
        if (loadGeneratorCount < 0) {
            throw new CommandLineExitException("loadgenerator_count must be a equal or larger than 0");
        }

        if (nodeCount == 0 && loadGeneratorCount == 0) {
            throw new CommandLineExitException("No workers have been defined!");
        }

        DeploymentPlan plan = new DeploymentPlan(registry.getAgents());
        plan.addAllProperty(properties.asMap());
        plan.addProperty("RUN_ID", coordinatorParameters.getRunId());
        plan.addToPlan(nodeCount, "member");
        plan.addToPlan(loadGeneratorCount, options.valueOf(clientTypeSpec));

        plan.printLayout();
        return plan;
    }

    private static File locateInventoryFile() {
        File file = preferredInventoryFile();

        if (!file.exists()) {
            throw new CommandLineExitException(format("Inventory file [%s] does not exist", file));
        }

        LOGGER.info("Loading inventory file: " + file.getAbsolutePath());
        return file;
    }

    private static File preferredInventoryFile() {
        File dir = getUserDir();
        for (; ; ) {
            File inventoryYamlFile = new File(dir, "inventory.yaml");
            if (inventoryYamlFile.exists()) {
                return inventoryYamlFile;
            }

            dir = dir.getParentFile();
            if (dir == null) {
                return new File(getUserDir(), "inventory.yaml");
            }
        }
    }

    public static void main(String[] args) {
        LOGGER.info("Hazelcast Simulator Coordinator");
        LOGGER.info(format("Version: %s, Commit: %s, Build Time: %s",
                getSimulatorVersion(), getCommitIdAbbrev(), getBuildTime()));
        LOGGER.info(format("SIMULATOR_HOME: %s", getSimulatorHome().getAbsolutePath()));

        try {
            CoordinatorCli cli = new CoordinatorCli(args);
            cli.run();
        } catch (Exception e) {
            exitWithError(LOGGER, "Failed to run Coordinator", e);
        }
    }
}
