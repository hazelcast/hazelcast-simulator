package com.hazelcast.simulator.test;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.tests.PropertiesTest;
import com.hazelcast.simulator.tests.SuccessTest;
import com.hazelcast.simulator.tests.TestContextImplTest;
import com.hazelcast.simulator.utils.FileUtils;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestRunnerTest {

    private final SuccessTest successTest = new SuccessTest();
    private final TestRunner<SuccessTest> testRunner = new TestRunner<SuccessTest>(successTest);

    @After
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    @Test(expected = NullPointerException.class)
    public void testConstructor_nullTest() {
        new TestRunner<SuccessTest>(null);
    }

    @Test
    public void testBasics() throws Exception {
        assertEquals(successTest, testRunner.getTest());
        assertNull(testRunner.getHazelcastInstance());
        assertTrue(testRunner.getDurationSeconds() > 0);

        testRunner.withDuration(3).withSleepInterval(1);
        assertEquals(3, testRunner.getDurationSeconds());

        testRunner.run();
        assertNotNull(testRunner.getHazelcastInstance());

        Set<TestPhase> testPhases = successTest.getTestPhases();
        assertEquals(TestPhase.values().length, testPhases.size());
    }

    @Test
    public void testWithProperties() throws Exception {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("testProperty", "testValue");

        PropertiesTest propertiesTest = new PropertiesTest();
        assertNull(propertiesTest.testProperty);

        TestRunner testRunner = new TestRunner<PropertiesTest>(propertiesTest, properties);

        testRunner.withDuration(1);
        assertEquals(1, testRunner.getDurationSeconds());

        testRunner.run();
        assertEquals("testValue", propertiesTest.testProperty);
    }

    @Test
    public void testTestContextImpl() throws Exception {
        TestContextImplTest test = new TestContextImplTest();
        TestRunner<TestContextImplTest> testRunner = new TestRunner<TestContextImplTest>(test);

        testRunner.withDuration(0).withHazelcastInstance(Hazelcast.newHazelcastInstance()).run();
    }

    @Test
    public void testWithDuration_zero() {
        testRunner.withDuration(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithDuration_negative() {
        testRunner.withDuration(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithSleepInterval_zero() {
        testRunner.withSleepInterval(0);
    }

    @Test(expected = NullPointerException.class)
    public void testWithHazelcastInstance_null() {
        testRunner.withHazelcastInstance(null);
    }

    @Test
    public void testWithHazelcastInstance() {
        HazelcastInstance hazelcastInstance = mock(HazelcastInstance.class);
        testRunner.withHazelcastInstance(hazelcastInstance);

        assertEquals(hazelcastInstance, testRunner.getHazelcastInstance());
    }

    @Test(expected = NullPointerException.class)
    public void withHazelcastConfigFile_null() throws Exception {
        testRunner.withHazelcastConfigFile(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void withHazelcastConfigFile_notFound() throws Exception {
        testRunner.withHazelcastConfigFile(new File("notFound"));
    }

    @Test
    public void withHazelcastConfigFile() throws Exception {
        File configFile = File.createTempFile("config", "xml");
        FileUtils.appendText("<hazelcast xsi:schemaLocation=\"http://www.hazelcast.com/schema/config\n" +
                "                               http://www.hazelcast.com/schema/config/hazelcast-config-3.6.xsd\"\n" +
                "           xmlns=\"http://www.hazelcast.com/schema/config\"\n" +
                "           xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">" +
                "</hazelcast>", configFile);

        testRunner.withHazelcastConfigFile(configFile);
    }

    @Test(expected = NullPointerException.class)
    public void withHazelcastConfig_null() {
        testRunner.withHazelcastConfig(null);
    }

    @Test
    public void withHazelcastConfig() {
        Config config = new Config();

        testRunner.withHazelcastConfig(config);
    }
}
