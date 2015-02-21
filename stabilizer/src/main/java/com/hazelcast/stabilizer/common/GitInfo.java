package com.hazelcast.stabilizer.common;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static com.hazelcast.stabilizer.utils.CommonUtils.closeQuietly;

public final class GitInfo {
    private static final String GIT_INFO_FILE = "stabilizer-git.properties";
    private static final String UNKNOWN = "Unknown";

    private static final String GIT_COMMIT_ID_AABREV = "git.commit.id.abbrev";
    private static final String GIT_COMMIT_ID = "git.commit.id";
    private static final String GIT_COMMIT_TIME = "git.commit.time";
    private static final String GIT_BUILD_TIME = "git.build.time";
    private static final String GIT_REMOTE_ORIGIN_URL = "git.remote.origin.url";

    private static final Logger log = Logger.getLogger(GitInfo.class);

    private final Properties properties;
    private final static GitInfo INSTANCE = new GitInfo();

    private GitInfo() {
        properties = loadGitProperties();
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

    private Properties loadGitProperties() {
        Properties properties = new Properties();
        InputStream gitPropsStream = null;
        try {
            gitPropsStream = getClass().getClassLoader().getResourceAsStream(GIT_INFO_FILE);
            properties.load(gitPropsStream);
        } catch (IOException e) {
            log.warn("Error while loading Git properties.", e);
            properties = new DummyProperties();
        } finally {
            closeQuietly(gitPropsStream);
        }
        return properties;
    }

    private static class DummyProperties extends Properties {
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
