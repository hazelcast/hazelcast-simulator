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

import com.hazelcast.simulator.cluster.WorkerConfigurationConverter;
import com.hazelcast.simulator.common.AgentsFile;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.protocol.registry.TargetType;
import com.hazelcast.simulator.test.FailureType;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.test.TestSuite;
import com.hazelcast.simulator.utils.CommandLineExitException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.common.GitInfo.getBuildTime;
import static com.hazelcast.simulator.common.GitInfo.getCommitIdAbbrev;
import static com.hazelcast.simulator.common.SimulatorProperties.PROPERTIES_FILE_NAME;
import static com.hazelcast.simulator.coordinator.WorkerParameters.initClientHzConfig;
import static com.hazelcast.simulator.coordinator.WorkerParameters.initMemberHzConfig;
import static com.hazelcast.simulator.test.FailureType.fromPropertyValue;
import static com.hazelcast.simulator.test.TestSuite.loadTestSuite;
import static com.hazelcast.simulator.utils.CliUtils.initOptionsWithHelp;
import static com.hazelcast.simulator.utils.CloudProviderUtils.isLocal;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.getFile;
import static com.hazelcast.simulator.utils.FileUtils.getFileAsTextFromWorkingDirOrBaseDir;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static com.hazelcast.simulator.utils.SimulatorUtils.loadComponentRegister;
import static com.hazelcast.simulator.utils.SimulatorUtils.loadSimulatorProperties;
import static java.lang.String.format;

final class CoordinatorCli {

    private static final Logger LOGGER = Logger.getLogger(CoordinatorCli.class);

    private final OptionParser parser = new OptionParser();

    private final OptionSpec<String> durationSpec = parser.accepts("duration",
            "Amount of time to execute run phase per test, e.g. 10s, 1m, 2h or 3d.")
            .withRequiredArg().ofType(String.class);

    private final OptionSpec waitForTestCaseSpec = parser.accepts("waitForTestCaseCompletion",
            "Wait for the TestCase to finish its run phase. Can be combined with --duration to limit runtime.");

    private final OptionSpec<String> overridesSpec = parser.accepts("overrides",
            "Properties that override the properties in a given test-case, e.g. --overrides"
                    + " \"threadcount=20,writeProb=0.2\". This makes it easy to parametrize a test.")
            .withRequiredArg().ofType(String.class).defaultsTo("");

    private final OptionSpec<Integer> memberWorkerCountSpec = parser.accepts("memberWorkerCount",
            "Number of cluster member Worker JVMs. If no value is specified and no mixed members are specified,"
                    + " then the number of cluster members will be equal to the number of machines in the agents file.")
            .withRequiredArg().ofType(Integer.class).defaultsTo(-1);

    private final OptionSpec<Integer> clientWorkerCountSpec = parser.accepts("clientWorkerCount",
            "Number of cluster client Worker JVMs.")
            .withRequiredArg().ofType(Integer.class).defaultsTo(0);

    private final OptionSpec<Integer> dedicatedMemberMachinesSpec = parser.accepts("dedicatedMemberMachines",
            "Controls the number of dedicated member machines. For example when there are 4 machines,"
                    + " 2 members and 9 clients with 1 dedicated member machine defined, then"
                    + " 1 machine gets the 2 members and the 3 remaining machines get 3 clients each.")
            .withRequiredArg().ofType(Integer.class).defaultsTo(0);

    private final OptionSpec<TargetType> targetTypeSpec = parser.accepts("targetType",
            format("Defines the type of Workers which execute the RUN phase."
                    + " The type PREFER_CLIENT selects client Workers if they are available, member Workers otherwise."
                    + " List of allowed types: %s", TargetType.getIdsAsString()))
            .withRequiredArg().ofType(TargetType.class).defaultsTo(TargetType.PREFER_CLIENT);

    private final OptionSpec<Integer> targetCountSpec = parser.accepts("targetCount",
            "Defines the number of Workers which execute the RUN phase. The value 0 selects all Workers.")
            .withRequiredArg().ofType(Integer.class).defaultsTo(0);

    private final OptionSpec<Boolean> autoCreateHzInstanceSpec = parser.accepts("autoCreateHzInstances",
            "Auto create Hazelcast instances.")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(true);

    private final OptionSpec<String> workerClassPathSpec = parser.accepts("workerClassPath",
            "A file/directory containing the classes/jars/resources that are going to be uploaded to the agents."
                    + " Use ';' as separator for multiple entries. The wildcard '*' can also be used.")
            .withRequiredArg().ofType(String.class);

    private final OptionSpec<String> testSuiteIdSpec = parser.accepts("testSuiteId",
            "Defines the ID of the testsuite. If not set the actual date will be used.")
            .withRequiredArg().ofType(String.class);

    private final OptionSpec monitorPerformanceSpec = parser.accepts("monitorPerformance",
            "If defined performance of tests is tracked.");

    private final OptionSpec<Boolean> verifyEnabledSpec = parser.accepts("verifyEnabled",
            "Defines if tests are verified.")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(true);

    private final OptionSpec<Boolean> workerRefreshSpec = parser.accepts("workerRefresh",
            "Defines if the Worker JVMs should be restarted after every test (in serial execution).")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(false);

    private final OptionSpec<Boolean> failFastSpec = parser.accepts("failFast",
            "Defines if the testsuite should fail immediately when a test from a testsuite fails instead of continuing.")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(true);

    private final OptionSpec<String> tolerableFailureSpec = parser.accepts("tolerableFailure",
            format("Defines if tests should not fail when given failure is detected. List of known failures: %s",
                    FailureType.getIdsAsString()))
            .withRequiredArg().ofType(String.class).defaultsTo("workerTimeout");

    private final OptionSpec parallelSpec = parser.accepts("parallel",
            "If defined tests are run in parallel.");

    private final OptionSpec<TestPhase> syncToTestPhaseSpec = parser.accepts("syncToTestPhase",
            format("Defines the last TestPhase which is synchronized between all parallel running tests."
                    + " Use --syncToTestPhase %s to synchronize all test phases."
                    + " List of defined test phases: %s", TestPhase.getLastTestPhase(), TestPhase.getIdsAsString()))
            .withRequiredArg().ofType(TestPhase.class).defaultsTo(TestPhase.SETUP);

    private final OptionSpec<String> workerVmOptionsSpec = parser.accepts("workerVmOptions",
            "Member Worker JVM options (quotes can be used).")
            .withRequiredArg().ofType(String.class).defaultsTo("-XX:+HeapDumpOnOutOfMemoryError");

    private final OptionSpec<String> clientWorkerVmOptionsSpec = parser.accepts("clientWorkerVmOptions",
            "Client Worker JVM options (quotes can be used).")
            .withRequiredArg().ofType(String.class).defaultsTo("-XX:+HeapDumpOnOutOfMemoryError");

    private final OptionSpec<String> agentsFileSpec = parser.accepts("agentsFile",
            "The file containing the list of Agent machines.")
            .withRequiredArg().ofType(String.class).defaultsTo(AgentsFile.NAME);

    private final OptionSpec<String> propertiesFileSpec = parser.accepts("propertiesFile",
            format("The file containing the simulator properties. If no file is explicitly configured,"
                            + " first the working directory is checked for a file '%s'."
                            + " All missing properties are always loaded from SIMULATOR_HOME/conf/%s",
                    PROPERTIES_FILE_NAME, PROPERTIES_FILE_NAME))
            .withRequiredArg().ofType(String.class);

    private final OptionSpec<String> memberHzConfigFileSpec = parser.accepts("hzFile",
            "The Hazelcast XML configuration file for the Worker. If no file is explicitly configured,"
                    + " first the 'hazelcast.xml' in the working directory is loaded."
                    + " If that doesn't exist then SIMULATOR_HOME/conf/hazelcast.xml is loaded.")
            .withRequiredArg().ofType(String.class).defaultsTo(getDefaultConfigurationFile("hazelcast.xml"));

    private final OptionSpec<String> clientHzConfigFileSpec = parser.accepts("clientHzFile",
            "The client Hazelcast XML configuration file for the Worker. If no file is explicitly configured,"
                    + " first the 'client-hazelcast.xml' in the working directory is loaded."
                    + " If that doesn't exist then SIMULATOR_HOME/conf/client-hazelcast.xml is loaded.")
            .withRequiredArg().ofType(String.class).defaultsTo(getDefaultConfigurationFile("client-hazelcast.xml"));

    private final OptionSpec<Integer> workerStartupTimeoutSpec = parser.accepts("workerStartupTimeout",
            "The startup timeout in seconds for a Worker.")
            .withRequiredArg().ofType(Integer.class).defaultsTo(60);

    private final OptionSpec<String> gitSpec = parser.accepts("git",
            "Overrides the HAZELCAST_VERSION_SPEC property and forces Provisioner to build Hazelcast JARs from a given Git"
                    + " version. This makes it easier to run a test with different versions of Hazelcast, e.g." + NEW_LINE
                    + "     --git f0288f713                to use the Git revision f0288f713" + NEW_LINE
                    + "     --git myRepository/myBranch    to use branch myBranch from a repository myRepository." + NEW_LINE
                    + "You can specify custom repositories in 'simulator.properties'.")
            .withRequiredArg().ofType(String.class);

    private final OptionSpec<Boolean> uploadHazelcastJARsSpec = parser.accepts("uploadHazelcastJARs",
            "Defines if the Hazelcast JARs should be uploaded.")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(true);

    private final OptionSpec<Boolean> enterpriseEnabledSpec = parser.accepts("enterpriseEnabled",
            "Use JARs of Hazelcast Enterprise Edition.")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(false);

    private final OptionSpec<String> licenseKeySpec = parser.accepts("licenseKey",
            "Sets the license key for Hazelcast Enterprise Edition.")
            .withRequiredArg().ofType(String.class);

    private CoordinatorCli() {
    }

    private static String getDefaultConfigurationFile(String filename) {
        File file = new File(filename).getAbsoluteFile();
        if (file.exists()) {
            return file.getAbsolutePath();
        } else {
            return getSimulatorHome() + "/conf/" + filename;
        }
    }

    static Coordinator init(String[] args) {
        LOGGER.info("Hazelcast Simulator Coordinator");
        LOGGER.info(format("Version: %s, Commit: %s, Build Time: %s", getSimulatorVersion(), getCommitIdAbbrev(),
                getBuildTime()));
        LOGGER.info(format("SIMULATOR_HOME: %s", getSimulatorHome()));

        CoordinatorCli cli = new CoordinatorCli();
        OptionSet options = initOptionsWithHelp(cli.parser, args);

        TestSuite testSuite = getTestSuite(cli, options);

        SimulatorProperties simulatorProperties = loadSimulatorProperties(options, cli.propertiesFileSpec);
        if (options.has(cli.gitSpec)) {
            simulatorProperties.forceGit(options.valueOf(cli.gitSpec));
        }

        ComponentRegistry componentRegistry = getComponentRegistry(cli, options, testSuite, simulatorProperties);

        CoordinatorParameters coordinatorParameters = new CoordinatorParameters(
                simulatorProperties,
                options.valueOf(cli.workerClassPathSpec),
                options.valueOf(cli.uploadHazelcastJARsSpec),
                options.valueOf(cli.enterpriseEnabledSpec),
                options.valueOf(cli.verifyEnabledSpec),
                options.has(cli.parallelSpec),
                options.valueOf(cli.workerRefreshSpec),
                options.valueOf(cli.targetTypeSpec),
                options.valueOf(cli.targetCountSpec),
                options.valueOf(cli.syncToTestPhaseSpec)
        );

        String memberHzConfig = loadMemberHzConfig(options, cli);
        String clientHzConfig = loadClientHzConfig(options, cli);
        int defaultHzPort = simulatorProperties.getHazelcastPort();
        String licenseKey = options.valueOf(cli.licenseKeySpec);

        WorkerParameters workerParameters = new WorkerParameters(
                simulatorProperties,
                options.valueOf(cli.autoCreateHzInstanceSpec),
                options.valueOf(cli.workerStartupTimeoutSpec),
                options.valueOf(cli.workerVmOptionsSpec),
                options.valueOf(cli.clientWorkerVmOptionsSpec),
                initMemberHzConfig(memberHzConfig, componentRegistry, defaultHzPort, licenseKey, simulatorProperties),
                initClientHzConfig(clientHzConfig, componentRegistry, defaultHzPort, licenseKey),
                loadLog4jConfig(),
                options.has(cli.monitorPerformanceSpec)
        );

        WorkerConfigurationConverter workerConfigurationConverter = new WorkerConfigurationConverter(defaultHzPort, licenseKey,
                workerParameters, simulatorProperties, componentRegistry);

        ClusterLayoutParameters clusterLayoutParameters = new ClusterLayoutParameters(
                loadClusterConfig(),
                workerConfigurationConverter,
                options.valueOf(cli.memberWorkerCountSpec),
                options.valueOf(cli.clientWorkerCountSpec),
                options.valueOf(cli.dedicatedMemberMachinesSpec),
                componentRegistry.agentCount()
        );
        if (clusterLayoutParameters.getDedicatedMemberMachineCount() < 0) {
            throw new CommandLineExitException("--dedicatedMemberMachines can't be smaller than 0");
        }

        return new Coordinator(testSuite, componentRegistry, coordinatorParameters, workerParameters, clusterLayoutParameters);
    }

    private static TestSuite getTestSuite(CoordinatorCli cli, OptionSet options) {
        TestSuite testSuite = loadTestSuite(getTestSuiteFile(options), options.valueOf(cli.overridesSpec),
                options.valueOf(cli.testSuiteIdSpec));
        testSuite.setDurationSeconds(getDurationSeconds(options, cli));
        testSuite.setWaitForTestCase(options.has(cli.waitForTestCaseSpec));
        testSuite.setFailFast(options.valueOf(cli.failFastSpec));
        testSuite.setTolerableFailures(fromPropertyValue(options.valueOf(cli.tolerableFailureSpec)));
        if (testSuite.getDurationSeconds() == 0 && !testSuite.isWaitForTestCase()) {
            throw new CommandLineExitException("You need to define --duration or --waitForTestCase or both!");
        }
        return testSuite;
    }

    private static ComponentRegistry getComponentRegistry(CoordinatorCli cli, OptionSet options, TestSuite testSuite,
                                                          SimulatorProperties simulatorProperties) {
        ComponentRegistry componentRegistry;
        if (isLocal(simulatorProperties)) {
            componentRegistry = new ComponentRegistry();
            componentRegistry.addAgent("localhost", "localhost");
        } else {
            componentRegistry = loadComponentRegister(getAgentsFile(cli, options));
        }
        componentRegistry.addTests(testSuite);
        return componentRegistry;
    }

    private static File getTestSuiteFile(OptionSet options) {
        File testSuiteFile;

        List testsuiteFiles = options.nonOptionArguments();
        if (testsuiteFiles.size() > 1) {
            throw new CommandLineExitException(format("Too many testsuite files specified: %s", testsuiteFiles));
        } else if (testsuiteFiles.size() == 1) {
            testSuiteFile = new File((String) testsuiteFiles.get(0));
        } else {
            testSuiteFile = new File("test.properties");
        }

        LOGGER.info("Loading testsuite file: " + testSuiteFile.getAbsolutePath());
        if (!testSuiteFile.exists()) {
            throw new CommandLineExitException(format("TestSuite file '%s' not found", testSuiteFile));
        }
        return testSuiteFile;
    }

    private static File getAgentsFile(CoordinatorCli cli, OptionSet options) {
        File file = getFile(cli.agentsFileSpec, options, "Agents file");
        LOGGER.info("Loading Agents file: " + file.getAbsolutePath());
        return file;
    }

    private static String loadMemberHzConfig(OptionSet options, CoordinatorCli cli) {
        File file = getFile(cli.memberHzConfigFileSpec, options, "Hazelcast member configuration for Worker");
        LOGGER.info("Loading Hazelcast member configuration: " + file.getAbsolutePath());
        return fileAsText(file);
    }

    private static String loadClientHzConfig(OptionSet options, CoordinatorCli cli) {
        File file = getFile(cli.clientHzConfigFileSpec, options, "Hazelcast client configuration for Worker");
        LOGGER.info("Loading Hazelcast client configuration: " + file.getAbsolutePath());
        return fileAsText(file);
    }

    private static String loadLog4jConfig() {
        return getFileAsTextFromWorkingDirOrBaseDir(getSimulatorHome(), "worker-log4j.xml", "Log4j configuration for Worker");
    }

    private static String loadClusterConfig() {
        File file = new File("cluster.xml").getAbsoluteFile();
        if (file.exists()) {
            LOGGER.info("Loading cluster configuration: " + file.getAbsolutePath());
            return fileAsText(file.getAbsolutePath());
        } else {
            return null;
        }
    }

    private static int getDurationSeconds(OptionSet options, CoordinatorCli cli) {
        if (!options.has(cli.durationSpec)) {
            return 0;
        }

        int duration;
        String value = options.valueOf(cli.durationSpec);
        try {
            if (value.endsWith("s")) {
                duration = parseDurationWithoutLastChar(TimeUnit.SECONDS, value);
            } else if (value.endsWith("m")) {
                duration = parseDurationWithoutLastChar(TimeUnit.MINUTES, value);
            } else if (value.endsWith("h")) {
                duration = parseDurationWithoutLastChar(TimeUnit.HOURS, value);
            } else if (value.endsWith("d")) {
                duration = parseDurationWithoutLastChar(TimeUnit.DAYS, value);
            } else {
                duration = Integer.parseInt(value);
            }
        } catch (NumberFormatException e) {
            throw new CommandLineExitException(format("Failed to parse duration '%s'", value), e);
        }

        if (duration < 1) {
            throw new CommandLineExitException("duration must be a positive number, but was: " + duration);
        }
        return duration;
    }

    private static int parseDurationWithoutLastChar(TimeUnit timeUnit, String value) {
        String sub = value.substring(0, value.length() - 1);
        return (int) timeUnit.toSeconds(Integer.parseInt(sub));
    }
}
