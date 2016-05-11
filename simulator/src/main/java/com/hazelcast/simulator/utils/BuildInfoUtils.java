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
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.Attributes.Name;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import static com.hazelcast.simulator.utils.FileUtils.getFilesFromClassPath;
import static com.hazelcast.simulator.utils.VersionUtils.parseVersionString;
import static java.lang.String.format;

public final class BuildInfoUtils {

    static final int DEFAULT_MAJOR_VERSION = 3;
    static final int DEFAULT_MINOR_VERSION = 6;

    static final int DEFAULT_FALLBACK_MAJOR_VERSION = 3;
    static final int DEFAULT_FALLBACK_MINOR_VERSION = 5;

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
        } catch (FileUtilsException ignored) {
            EmptyStatement.ignore(ignored);
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
        try {
            JarFile jar = new JarFile(jarFile);
            Manifest manifest = jar.getManifest();

            String versionNumber = null;
            Attributes attributes = manifest.getMainAttributes();
            if (attributes != null) {
                for (Object attributeKey : attributes.keySet()) {
                    Name key = (Name) attributeKey;
                    String keyword = key.toString();
                    if (keyword.equals("Implementation-Version") || keyword.equals("Bundle-Version")) {
                        versionNumber = (String) attributes.get(key);
                        break;
                    }
                }
            }
            jar.close();
            return versionNumber;
        } catch (Exception ignored) {
            return null;
        }
    }
}
