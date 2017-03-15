package com.hazelcast.simulator.vendors;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;

import static com.hazelcast.simulator.TestEnvironmentUtils.createAgentsFileWithLocalhost;
import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.TestEnvironmentUtils.tearDownFakeEnvironment;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;

public class HazelcastDriverTest {

    private static final String HAZELCAST_XML = "<hazelcast xsi:schemaLocation=\"http://www.hazelcast.com/schema/config"
            + NEW_LINE + "  http://www.hazelcast.com/schema/config/hazelcast-config-3.8.xsd\""
            + NEW_LINE + "  xmlns=\"http://www.hazelcast.com/schema/config\""
            + NEW_LINE + "  xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" />";


    private static File propertiesFile;

    @BeforeClass
    public static void beforeClass() throws Exception {
        setupFakeEnvironment();

        createAgentsFileWithLocalhost();
        propertiesFile = ensureExistingFile("simulator.properties");
    }

    @AfterClass
    public static void afterClass() {
        tearDownFakeEnvironment();
    //    deleteQuiet(testSuiteFile);
        deleteQuiet(propertiesFile);
    }

//    @After
//    public void after() {
//        deleteQuiet(new File(sessionId).getAbsoluteFile());
//    }


//    @Test
//    public void testInit_memberConfigFileInWorkDir() {
//        File memberConfigFile = new File("hazelcast.xml").getAbsoluteFile();
//        writeText(HAZELCAST_XML, memberConfigFile);
//
//        args.add(testSuiteFile.getAbsolutePath());
//
//        try {
//            CoordinatorCli cli = createCoordinatorCli();
//            assertEquals(HAZELCAST_XML, cli.workerParametersMap.get(WorkerType.MEMBER).getEnvironment().get("HAZELCAST_CONFIG"));
//        } finally {
//            deleteQuiet(memberConfigFile);
//        }
//    }
//
//    @Test
//    public void testInit_clientConfigFileInWorkDir() {
//        File clientConfigFile = new File("client-hazelcast.xml").getAbsoluteFile();
//        writeText(HAZELCAST_XML, clientConfigFile);
//
//        args.add(testSuiteFile.getAbsolutePath());
//        try {
//            CoordinatorCli cli = createCoordinatorCli();
//            assertEquals(HAZELCAST_XML, cli.workerParametersMap.get(WorkerType.JAVA_CLIENT).getEnvironment().get("HAZELCAST_CONFIG"));
//        } finally {
//            deleteQuiet(clientConfigFile);
//        }
//    }
}
