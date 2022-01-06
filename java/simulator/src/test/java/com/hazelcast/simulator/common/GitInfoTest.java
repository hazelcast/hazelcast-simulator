package com.hazelcast.simulator.common;

import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class GitInfoTest {

    @Test
    public void testGetCommitIdAbbrev() throws Exception {
        assertNotNull(GitInfo.getCommitIdAbbrev());
    }

    @Test
    public void testGetCommitId() throws Exception {
        assertNotNull(GitInfo.getCommitId());
    }

    @Test
    public void testGetCommitTime() throws Exception {
        assertNotNull(GitInfo.getCommitTime());
    }

    @Test
    public void testGetBuildTime() throws Exception {
        assertNotNull(GitInfo.getBuildTime());
    }

    @Test
    public void testGetRemoteOriginUrl() throws Exception {
        assertNotNull(GitInfo.getRemoteOriginUrl());
    }

    @Test
    public void testLoadProperties() {
        Properties properties = GitInfo.loadGitProperties(GitInfo.GIT_INFO_FILE);
        assertNotNull(properties);
        assertFalse(properties instanceof GitInfo.UnknownGitProperties);
    }

    @Test
    public void testLoadProperties_notExists() {
        Properties properties = GitInfo.loadGitProperties("notExists");
        assertNotNull(properties);
        assertTrue(properties instanceof GitInfo.UnknownGitProperties);

        assertEquals(GitInfo.UNKNOWN, properties.getProperty(GitInfo.GIT_COMMIT_ID_AABREV));
        assertEquals(GitInfo.UNKNOWN, properties.getProperty(GitInfo.GIT_COMMIT_ID));
        assertEquals(GitInfo.UNKNOWN, properties.getProperty(GitInfo.GIT_COMMIT_TIME));
        assertEquals(GitInfo.UNKNOWN, properties.getProperty(GitInfo.GIT_BUILD_TIME));
        assertEquals(GitInfo.UNKNOWN, properties.getProperty(GitInfo.GIT_REMOTE_ORIGIN_URL));

        assertEquals("default", properties.getProperty(GitInfo.GIT_COMMIT_ID_AABREV, "default"));
    }

    @Test(expected = NullPointerException.class)
    public void testLoadProperties_null() {
        GitInfo.loadGitProperties(null);
    }
}
