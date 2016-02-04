package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.cluster.ClusterLayout;
import com.hazelcast.simulator.cluster.WorkerConfigurationConverter;
import com.hazelcast.simulator.common.JavaProfiler;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.test.TestException;
import com.hazelcast.simulator.utils.Bash;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.jars.HazelcastJARs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashSet;

import static com.hazelcast.simulator.common.JavaProfiler.NONE;
import static com.hazelcast.simulator.common.JavaProfiler.YOURKIT;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static com.hazelcast.simulator.utils.jars.HazelcastJARs.OUT_OF_THE_BOX;
import static java.util.Collections.singleton;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.contains;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class CoordinatorUploaderTest {

    private ComponentRegistry componentRegistry = new ComponentRegistry();
    private ClusterLayout clusterLayout;

    private Bash bash = mock(Bash.class);
    private HazelcastJARs hazelcastJARs = mock(HazelcastJARs.class);
    private WorkerParameters workerParameters = mock(WorkerParameters.class);

    private String testSuiteId = "testSuiteId";

    private File notExists = new File("/notExists");
    private File uploadDirectory = ensureExistingDirectory("upload");
    private File workerClassPathFile = ensureExistingDirectory("workerClassPath");
    private String workerClassPath = workerClassPathFile.getAbsolutePath();

    private CoordinatorUploader coordinatorUploader;

    @Before
    public void setUp() {
        componentRegistry.addAgent("192.168.0.1", "192.168.0.1");
        componentRegistry.addAgent("192.168.0.2", "192.168.0.2");

        when(workerParameters.getHazelcastVersionSpec()).thenReturn(OUT_OF_THE_BOX);
        when(workerParameters.getProfiler()).thenReturn(JavaProfiler.NONE);

        ClusterLayoutParameters clusterLayoutParameters = new ClusterLayoutParameters(null, null, 2, 0, 0, 2);

        clusterLayout = new ClusterLayout(componentRegistry, workerParameters, clusterLayoutParameters);

        coordinatorUploader = new CoordinatorUploader(bash, componentRegistry, clusterLayout, hazelcastJARs, true, false,
                workerClassPath, YOURKIT, testSuiteId);
    }

    @After
    public void tearDown() {
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
        verify(hazelcastJARs, times(2)).upload(contains("192.168.0."), anyString(), eq(singleton(OUT_OF_THE_BOX)));
        verifyNoMoreInteractions(hazelcastJARs);
        verifyNoMoreInteractions(bash);
    }

    @Test
    public void testUploadHazelcastJARs_withClusterXml() {
        String xml
                = "<clusterConfiguration>"
                + NEW_LINE + "\t<workerConfiguration name=\"hz351\" type=\"MEMBER\" hzVersion=\"maven=3.5.1\"/>"
                + NEW_LINE + "\t<workerConfiguration name=\"hz352\" type=\"MEMBER\" hzVersion=\"maven=3.5.2\"/>"
                + NEW_LINE + "\t<nodeConfiguration>"
                + NEW_LINE + "\t\t<workerGroup configuration=\"hz351\" count=\"1\"/>"
                + NEW_LINE + "\t\t<workerGroup configuration=\"hz352\" count=\"1\"/>"
                + NEW_LINE + "\t</nodeConfiguration>"
                + NEW_LINE + "\t<nodeConfiguration>"
                + NEW_LINE + "\t\t<workerGroup configuration=\"hz352\" count=\"1\"/>"
                + NEW_LINE + "\t</nodeConfiguration>"
                + NEW_LINE + "</clusterConfiguration>";

        SimulatorProperties simulatorProperties = mock(SimulatorProperties.class);
        WorkerConfigurationConverter converter = new WorkerConfigurationConverter(5701, null, workerParameters,
                simulatorProperties, componentRegistry);
        ClusterLayoutParameters clusterLayoutParameters = new ClusterLayoutParameters(xml, converter, 0, 0, 0, 2);

        clusterLayout = new ClusterLayout(componentRegistry, workerParameters, clusterLayoutParameters);
        coordinatorUploader = new CoordinatorUploader(bash, componentRegistry, clusterLayout, hazelcastJARs, true, false,
                workerClassPath, YOURKIT, testSuiteId);

        coordinatorUploader.uploadHazelcastJARs();

        HashSet<String> versionSpecs = new HashSet<String>(2);
        versionSpecs.add("maven=3.5.1");
        versionSpecs.add("maven=3.5.2");

        verify(hazelcastJARs, times(1)).prepare(false);
        verify(hazelcastJARs, times(1)).upload(contains("192.168.0.1"), anyString(), eq(versionSpecs));
        verify(hazelcastJARs, times(1)).upload(contains("192.168.0.2"), anyString(), eq(singleton("maven=3.5.2")));
        verifyNoMoreInteractions(hazelcastJARs);
        verifyNoMoreInteractions(bash);
    }

    @Test
    public void testUploadHazelcastJARs_isNull() {
        coordinatorUploader = new CoordinatorUploader(bash, componentRegistry, clusterLayout, null, true, false, workerClassPath,
                YOURKIT, testSuiteId);

        coordinatorUploader.uploadHazelcastJARs();

        verifyNoMoreInteractions(bash);
    }

    @Test
    public void testUploadUploadDirectory() {
        coordinatorUploader.uploadUploadDirectory();

        verify(bash, times(2)).ssh(contains("192.168.0."), anyString());
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
        TestException exception = new TestException("expected");
        doThrow(exception).when(bash).uploadToRemoteSimulatorDir(contains("192.168.0."), anyString(), anyString());

        coordinatorUploader.uploadUploadDirectory();
    }

    @Test
    public void testUploadWorkerClassPath() {
        coordinatorUploader.uploadWorkerClassPath();

        verify(bash, times(2)).ssh(contains("192.168.0."), anyString());
        verify(bash, times(2)).uploadToRemoteSimulatorDir(contains("192.168.0."), anyString(), anyString());
        verifyNoMoreInteractions(bash);
    }

    @Test
    public void testUploadWorkerClassPath_workerClassPathIsNull() {
        coordinatorUploader = new CoordinatorUploader(bash, componentRegistry, clusterLayout, hazelcastJARs, true, false, null,
                YOURKIT, testSuiteId);

        coordinatorUploader.uploadWorkerClassPath();

        verifyNoMoreInteractions(bash);
    }

    @Test(expected = CommandLineExitException.class)
    public void testUploadWorkerClassPath_workerClassPathNotExists() {
        coordinatorUploader = new CoordinatorUploader(bash, componentRegistry, clusterLayout, hazelcastJARs, true, false,
                notExists.getAbsolutePath(), YOURKIT, testSuiteId);

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
        coordinatorUploader = new CoordinatorUploader(bash, componentRegistry, clusterLayout, hazelcastJARs, true, false,
                workerClassPath, NONE, testSuiteId);

        coordinatorUploader.uploadYourKit();

        verifyNoMoreInteractions(bash);
    }
}
