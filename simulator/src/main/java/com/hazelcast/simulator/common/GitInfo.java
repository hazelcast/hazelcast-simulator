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

import org.apache.log4j.Logger;

import java.io.InputStream;
import java.util.Properties;

import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;

public final class GitInfo {

    static final String GIT_INFO_FILE = "simulator-git.properties";

    static final String UNKNOWN = "Unknown";

    static final String GIT_COMMIT_ID_AABREV = "git.commit.id.abbrev";
    static final String GIT_COMMIT_ID = "git.commit.id";
    static final String GIT_COMMIT_TIME = "git.commit.time";
    static final String GIT_BUILD_TIME = "git.build.time";
    static final String GIT_REMOTE_ORIGIN_URL = "git.remote.origin.url";

    private static final Logger LOGGER = Logger.getLogger(GitInfo.class);

    private static final GitInfo INSTANCE = new GitInfo();

    private final Properties properties;

    private GitInfo() {
        properties = loadGitProperties(GIT_INFO_FILE);
    }

    public static String getCommitIdAbbrev() {
        return INSTANCE.properties.getProperty(GIT_COMMIT_ID_AABREV, UNKNOWN);
    }

    public static String getCommitId() {
        return INSTANCE.properties.getProperty(GIT_COMMIT_ID, UNKNOWN);
    }

    public static String getCommitTime() {
        return INSTANCE.properties.getProperty(GIT_COMMIT_TIME, UNKNOWN);
    }

    public static String getBuildTime() {
        return INSTANCE.properties.getProperty(GIT_BUILD_TIME, UNKNOWN);
    }

    public static String getRemoteOriginUrl() {
        return INSTANCE.properties.getProperty(GIT_REMOTE_ORIGIN_URL, UNKNOWN);
    }

    static Properties loadGitProperties(String fileName) {
        Properties properties = new Properties();
        InputStream inputStream = GitInfo.class.getClassLoader().getResourceAsStream(fileName);
        try {
            properties.load(inputStream);
            return properties;
        } catch (NullPointerException e) {
            LOGGER.trace("Error while loading Git properties from " + fileName, e);
        } catch (Exception e) {
            LOGGER.warn("Error while loading Git properties from " + fileName, e);
        } finally {
            closeQuietly(inputStream);
        }
        return new UnknownGitProperties();
    }

    static class UnknownGitProperties extends Properties {

        @Override
        public String getProperty(String key) {
            return UNKNOWN;
        }

        @Override
        public String getProperty(String key, String defaultValue) {
            return defaultValue;
        }
    }
}
