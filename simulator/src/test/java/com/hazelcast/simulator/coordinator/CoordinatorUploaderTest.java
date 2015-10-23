package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.test.TestException;
import com.hazelcast.simulator.utils.Bash;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.jars.HazelcastJARs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.common.JavaProfiler.NONE;
import static com.hazelcast.simulator.common.JavaProfiler.YOURKIT;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.contains;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class CoordinatorUploaderTest {

    private ComponentRegistry componentRegistry = new ComponentRegistry();

    private Bash bash = mock(Bash.class);
    private HazelcastJARs hazelcastJARs = mock(HazelcastJARs.class);

    private String testSuiteId = "testSuiteId";
    private String simulatorHome = getSimulatorHome().getAbsolutePath();

    private File notExists = new File("/notExists");
    private File uploadDirectory = new File("./upload");
    private File workerClassPathFile = new File("./workerClassPath");
    private String workerClassPath = workerClassPathFile.getAbsolutePath();

    private CoordinatorUploader coordinatorUploader;

    @Before
    public void setUp() throws Exception {
        componentRegistry.addAgent("192.168.0.1", "192.168.0.1");
        componentRegistry.addAgent("192.168.0.2", "192.168.0.2");

        ensureExistingDirectory(uploadDirectory);
        ensureExistingDirectory(workerClassPathFile);

        coordinatorUploader = new CoordinatorUploader(
                componentRegistry, bash,
                testSuiteId, simulatorHome,
                hazelcastJARs, false,
                workerClassPath,
                YOURKIT
        );
    }

    @After
    public void tearDown() throws Exception {
        deleteQuiet(uploadDirectory);
        deleteQuiet(workerClassPathFile);
    }

    @Test
    public void testRun() {
        coordinatorUploader.run();
    }

    @Test
    public void testUploadHazelcastJARs() {
        coordinatorUploader.uploadHazelcastJARs();

        verify(hazelcastJARs, times(1)).prepare(false);
        verify(hazelcastJARs, times(2)).upload(contains("192.168.0."), anyString());
        verifyNoMoreInteractions(hazelcastJARs);

        verifyNoMoreInteractions(bash);
    }

    @Test
    public void testUploadUploadDirectory() {
        coordinatorUploader.uploadUploadDirectory();

        verify(bash, times(2)).uploadToRemoteSimulatorDir(contains("192.168.0."), anyString(), anyString());
        verifyNoMoreInteractions(bash);
    }

    @Test
    public void testUploadUploadDirectory_uploadDirectoryNotExists() {
        deleteQuiet(uploadDirectory);
        coordinatorUploader.uploadUploadDirectory();

        verifyNoMoreInteractions(bash);
    }

    @Test(expected = CommandLineExitException.class)
    public void testUploadUploadDirectory_withException() {
        doThrow(new TestException("expected")).when(bash).uploadToRemoteSimulatorDir(
                contains("192.168.0."), anyString(), anyString());
        coordinatorUploader.uploadUploadDirectory();
    }

    @Test
    public void testUploadWorkerClassPath() {
        coordinatorUploader.uploadWorkerClassPath();

        verify(bash, times(2)).uploadToRemoteSimulatorDir(contains("192.168.0."), anyString(), anyString());
        verifyNoMoreInteractions(bash);
    }

    @Test
    public void testUploadWorkerClassPath_workerClassPathIsNull() {
        coordinatorUploader = new CoordinatorUploader(componentRegistry, bash, testSuiteId, simulatorHome, hazelcastJARs, false,
                null, YOURKIT);

        coordinatorUploader.uploadWorkerClassPath();

        verifyNoMoreInteractions(bash);
    }

    @Test(expected = CommandLineExitException.class)
    public void testUploadWorkerClassPath_workerClassPathNotExists() {
        coordinatorUploader = new CoordinatorUploader(componentRegistry, bash, testSuiteId, simulatorHome, hazelcastJARs, false,
                notExists.getAbsolutePath(), YOURKIT);

        coordinatorUploader.uploadWorkerClassPath();
    }

    @Test
    public void testUploadYourKit() {
        coordinatorUploader.uploadYourKit();

        verify(bash, times(2)).ssh(contains("192.168.0."), anyString());
        verify(bash, times(2)).uploadToRemoteSimulatorDir(contains("192.168.0."), anyString(), anyString());
        verifyNoMoreInteractions(bash);
    }

    @Test
    public void testUploadYourKit_noYourKitProfiler() {
        coordinatorUploader = new CoordinatorUploader(componentRegistry, bash, testSuiteId, simulatorHome, hazelcastJARs, false,
                workerClassPath, NONE);

        coordinatorUploader.uploadYourKit();

        verifyNoMoreInteractions(bash);
    }
}
