package com.hazelcast.simulator.test;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.tests.SuccessTest;
import com.hazelcast.simulator.tests.TestContextImplTest;
import com.hazelcast.simulator.utils.FileUtils;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestRunnerTest {

    private final SuccessTest successTest = new SuccessTest();
    private final TestRunner<SuccessTest> testRunner = new TestRunner<SuccessTest>(successTest);

    @Test(expected = NullPointerException.class)
    public void testConstructor_nullTest() {
        new TestRunner<SuccessTest>(null);
    }

    @Test
    public void testBasics() throws Throwable {
        assertEquals(successTest, testRunner.getTest());
        assertNull(testRunner.getHazelcastInstance());
        assertTrue(testRunner.getDurationSeconds() > 0);

        testRunner.withDuration(5);
        assertEquals(5, testRunner.getDurationSeconds());

        testRunner.run();
        assertNotNull(testRunner.getHazelcastInstance());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testTestContextImpl() throws Throwable {
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
    public void testWithHazelcastConfig_null() throws Exception {
        testRunner.withHazelcastConfig(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithHazelcastConfig_notFound() throws Exception {
        testRunner.withHazelcastConfig(new File("notFound"));
    }

    @Test
    public void testWithHazelcastConfig() throws Exception {
        File configFile = File.createTempFile("config", "xml");
        FileUtils.appendText("<hazelcast />", configFile);

        testRunner.withHazelcastConfig(configFile);
    }
}