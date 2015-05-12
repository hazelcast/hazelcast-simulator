package com.hazelcast.simulator.common;

import org.apache.log4j.Logger;

import java.io.IOException;
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
        InputStream gitPropsStream = null;
        try {
            gitPropsStream = GitInfo.class.getClassLoader().getResourceAsStream(fileName);
            if (gitPropsStream == null) {
                return new DummyProperties();
            }
            Properties properties = new Properties();
            properties.load(gitPropsStream);
            return properties;
        } catch (IOException e) {
            LOGGER.warn("Error while loading Git properties from " + fileName, e);
            return new DummyProperties();
        } finally {
            closeQuietly(gitPropsStream);
        }
    }

    static class DummyProperties extends Properties {

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
