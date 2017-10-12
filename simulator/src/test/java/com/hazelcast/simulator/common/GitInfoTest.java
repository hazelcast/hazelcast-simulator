/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.common;

import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class GitInfoTest {

    @Test
    public void testGetCommitIdAbbrev() {
        assertNotNull(GitInfo.getCommitIdAbbrev());
    }

    @Test
    public void testGetCommitId() {
        assertNotNull(GitInfo.getCommitId());
    }

    @Test
    public void testGetCommitTime() {
        assertNotNull(GitInfo.getCommitTime());
    }

    @Test
    public void testGetBuildTime() {
        assertNotNull(GitInfo.getBuildTime());
    }

    @Test
    public void testGetRemoteOriginUrl() {
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
