package com.hazelcast.simulator.utils.jars;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.provisioner.Bash;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.junit.After;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.jars.HazelcastJARs.BRING_MY_OWN;
import static com.hazelcast.simulator.utils.jars.HazelcastJARs.OUT_OF_THE_BOX;
import static com.hazelcast.simulator.utils.jars.HazelcastJARs.directoryForVersionSpec;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.contains;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class HazelcastJARsTest {

    private Bash bash = mock(Bash.class);
    private GitSupport gitSupport = mock(GitSupport.class);

    @After
    public void tearDown() {
        deleteQuiet(new File("./logs"));
    }

    @Test
    public void testNewInstance() {
        SimulatorProperties properties = mock(SimulatorProperties.class);
        HazelcastJARs.newInstance(bash, properties);
    }

    @Test
    public void testDirectoryForVersionSpec() {
        assertNull(directoryForVersionSpec(BRING_MY_OWN));
        assertEquals("outofthebox", directoryForVersionSpec(OUT_OF_THE_BOX));
        assertEquals("git-tag-3.6", directoryForVersionSpec("git=tag=3.6"));
        assertEquals("maven-3.6", directoryForVersionSpec("maven=3.6"));
    }

    @Test
    public void testPrepare_outOfTheBox() {
        HazelcastJARs hazelcastJARs = getHazelcastJARs(OUT_OF_THE_BOX);
        hazelcastJARs.prepare(false);
    }

    @Test
    public void testPrepare_bringMyOwn() {
        HazelcastJARs hazelcastJARs = getHazelcastJARs(BRING_MY_OWN);
        hazelcastJARs.prepare(false);
    }

    @Test
    public void testPrepare_git() {
        when(gitSupport.checkout("tag=8.7")).thenReturn(new File[]{});

        HazelcastJARs hazelcastJARs = getHazelcastJARs("git=tag=8.7");
        hazelcastJARs.prepare(false);
    }

    @Test(expected = CommandLineExitException.class)
    public void testPrepare_git_enterpriseEnabled() {
        HazelcastJARs hazelcastJARs = getHazelcastJARs("git=tag=3.5.3");
        hazelcastJARs.prepare(true);
    }

    @Test
    public void testPrepare_maven_existingRelease() {
        HazelcastJARs hazelcastJARs = getHazelcastJARs("maven=8.7");
        File artifactFile = hazelcastJARs.getArtifactFile("hazelcast", "8.7");
        File artifactDir = new File(artifactFile.getParent());
        try {
            ensureExistingDirectory(artifactDir);
            ensureExistingFile(artifactFile);

            hazelcastJARs.prepare(false);
        } finally {
            deleteQuiet(artifactFile);
            deleteQuiet(artifactDir);
        }

        verify(bash, times(1)).execute(anyString());
        verify(bash, atLeastOnce()).download(anyString(), anyString());
        verifyNoMoreInteractions(bash);
    }

    @Test
    public void testPrepare_maven_invalidRelease_enterpriseEnabled() {
        HazelcastJARs hazelcastJARs = getHazelcastJARs("maven=8.7");
        hazelcastJARs.prepare(true);

        verify(bash, atLeastOnce()).download(anyString(), anyString());
        verifyNoMoreInteractions(bash);
    }

    @Test(expected = CommandLineExitException.class)
    public void testPrepare_maven_invalidSNAPSHOT() {
        HazelcastJARs hazelcastJARs = getHazelcastJARs("maven=8.7-SNAPSHOT");
        hazelcastJARs.prepare(false);
    }

    @Test(expected = CommandLineExitException.class)
    public void testPrepare_invalidVersionSpec() {
        HazelcastJARs hazelcastJARs = getHazelcastJARs("invalidSpec");
        hazelcastJARs.prepare(false);
    }

    @Test
    public void testPurge() {
        HazelcastJARs hazelcastJARs = getHazelcastJARs(OUT_OF_THE_BOX);

        hazelcastJARs.purge("127.0.0.1");

        verify(bash, times(1)).sshQuiet(eq("127.0.0.1"), anyString());
        verifyNoMoreInteractions(bash);
    }

    @Test
    public void testUpload() {
        HazelcastJARs hazelcastJARs = getHazelcastJARs("maven=3.6");
        String sourceDir = hazelcastJARs.getAbsolutePath("maven=3.6");
        String targetDir = directoryForVersionSpec("maven=3.6");

        hazelcastJARs.upload("127.0.0.1", "simulatorHome");

        verify(bash, times(1)).ssh(eq("127.0.0.1"), contains(targetDir));
        verify(bash, times(1)).uploadToAgentSimulatorDir(eq("127.0.0.1"), contains(sourceDir), anyString());
        verifyNoMoreInteractions(bash);
    }

    @Test
    public void testUpload_outOfTheBox() {
        String targetDir = directoryForVersionSpec(OUT_OF_THE_BOX);
        HazelcastJARs hazelcastJARs = getHazelcastJARs(OUT_OF_THE_BOX);
        hazelcastJARs.upload("127.0.0.1", "simulatorHome");

        verify(bash, times(1)).ssh(eq("127.0.0.1"), contains(targetDir));
        verify(bash, times(1)).uploadToAgentSimulatorDir(eq("127.0.0.1"), contains("simulatorHome"), anyString());
        verifyNoMoreInteractions(bash);
    }

    @Test
    public void testUpload_bringMyOwn() {
        HazelcastJARs hazelcastJARs = getHazelcastJARs(BRING_MY_OWN);
        hazelcastJARs.upload("127.0.0.1", getSimulatorHome().getAbsolutePath());

        verifyNoMoreInteractions(bash);
    }

    @Test
    public void testGetSnapshotUrl() {
        HazelcastJARs hazelcastJARs = getHazelcastJARs();

        String url = hazelcastJARs.getSnapshotUrl("hazelcast", "3.6-SNAPSHOT");
        assertTrue(url.contains("hazelcast"));
        assertTrue(url.contains("3.6-SNAPSHOT"));
    }

    @Test(expected = CommandLineExitException.class)
    public void testGetSnapshotUrl_invalidVersion() {
        HazelcastJARs hazelcastJARs = getHazelcastJARs();

        hazelcastJARs.getSnapshotUrl("hazelcast", "8.7-SNAPSHOT");
    }

    @Test
    public void testGetReleaseUrl() {
        HazelcastJARs hazelcastJARs = getHazelcastJARs();

        String url = hazelcastJARs.getReleaseUrl("hazelcast", "3.5.3");
        assertTrue(url.contains("hazelcast"));
        assertTrue(url.contains("3.5.3"));
    }

    @Test
    public void testGetTagValue() {
        HazelcastJARs hazelcastJARs = getHazelcastJARs();

        String buildNumber = hazelcastJARs.getTagValue(getMavenMetadata(), "buildNumber");
        assertEquals("565", buildNumber);
    }

    @Test(expected = CommandLineExitException.class)
    public void testGetTagValue_invalidTag() {
        HazelcastJARs hazelcastJARs = getHazelcastJARs();

        hazelcastJARs.getTagValue(getMavenMetadata(), "notFound");
    }

    private HazelcastJARs getHazelcastJARs() {
        return getHazelcastJARs(OUT_OF_THE_BOX);
    }

    private HazelcastJARs getHazelcastJARs(String version) {
        return new HazelcastJARs(bash, gitSupport, version);
    }

    private String getMavenMetadata() {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<metadata>  <groupId>com.hazelcast</groupId>" +
                "  <artifactId>hazelcast</artifactId>" +
                "  <version>3.6-SNAPSHOT</version>" +
                "  <versioning>" +
                "    <snapshot>" +
                "      <timestamp>20151018.215739</timestamp>" +
                "      <buildNumber>565</buildNumber>" +
                "    </snapshot>" +
                "    <lastUpdated>20151018215739</lastUpdated>" +
                "  </versioning>" +
                "</metadata>";
    }
}
