/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.utils;

import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;

import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.EmptyStatement.ignore;
import static com.hazelcast.simulator.utils.FileUtils.getFilesFromClassPath;
import static com.hazelcast.simulator.utils.VersionUtils.parseVersionString;
import static java.lang.String.format;

public final class BuildInfoUtils {

    static final int DEFAULT_MAJOR_VERSION = 5;
    static final int DEFAULT_MINOR_VERSION = 0;

    static final int DEFAULT_FALLBACK_MAJOR_VERSION = 5;
    static final int DEFAULT_FALLBACK_MINOR_VERSION = 0;

    private BuildInfoUtils() {
    }

    public static boolean isMinVersion(String minVersion) {
        return isMinVersion(minVersion, getVersion());
    }

    public static String getHazelcastVersionFromJAR(String classPath) {
        String hzVersion = getHazelcastVersionFromJarOrNull(classPath);
        int majorVersion = (hzVersion != null) ? getMajorVersion(hzVersion) : DEFAULT_MAJOR_VERSION;
        int minorVersion = (hzVersion != null) ? getMinorVersion(hzVersion) : DEFAULT_MINOR_VERSION;
        return format("%d%d", majorVersion, minorVersion);
    }

    static boolean isMinVersion(String minVersion, String version) {
        if (version == null) {
            return false;
        }
        return VersionUtils.isMinVersion(minVersion, getVersion());
    }

    static String getHazelcastVersionFromJarOrNull(String classPath) {
        try {
            List<File> jarFiles = getFilesFromClassPath(classPath);
            for (File jarFile : jarFiles) {
                String version = getVersion(jarFile);
                if (version != null) {
                    return version;
                }
            }
        } catch (UncheckedIOException ignored) {
            ignore(ignored);
        }
        return null;
    }

    static int getMajorVersion() {
        return getMajorVersion(getVersion());
    }

    static int getMinorVersion() {
        return getMinorVersion(getVersion());
    }

    static int getMajorVersion(String version) {
        if (version == null) {
            return DEFAULT_FALLBACK_MAJOR_VERSION;
        }
        String[] versions = parseVersionString(version);
        return Integer.parseInt(versions[0]);
    }

    static int getMinorVersion(String version) {
        if (version == null) {
            return DEFAULT_FALLBACK_MINOR_VERSION;
        }
        String[] versions = parseVersionString(version);
        return Integer.parseInt(versions[1]);
    }

    private static String getVersion() {
        try {
            BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();
            return buildInfo.getVersion();
        } catch (NoClassDefFoundError e) {
            // it's Hazelcast 3.2 or older
            return null;
        }
    }

    private static String getVersion(File jarFile) {
        JarFile jar = null;
        InputStream is = null;
        try {
            jar = new JarFile(jarFile);

            JarEntry entry = jar.getJarEntry("hazelcast-runtime.properties");
            if (entry != null) {
                is = jar.getInputStream(entry);
                Properties properties = new Properties();
                properties.load(is);
                String hazelcastVersion = properties.getProperty("hazelcast.version");
                if (hazelcastVersion != null) {
                    return hazelcastVersion;
                }
            }

            Attributes attributes = jar.getManifest().getMainAttributes();
            if (attributes != null) {
                for (Map.Entry<Object, Object> attributeEntry : attributes.entrySet()) {
                    String key = attributeEntry.getKey().toString();
                    if (key.equals("Implementation-Version") || key.equals("Bundle-Version")) {
                        return (String) attributeEntry.getValue();
                    }
                }
            }
        } catch (Exception e) {
            ignore(e);
        } finally {
            closeQuietly(is);
            closeQuietly(jar);
        }
        return null;
    }
}
