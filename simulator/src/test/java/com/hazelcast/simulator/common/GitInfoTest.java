package com.hazelcast.simulator.common;

import org.junit.Test;

import static org.junit.Assert.*;

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
}