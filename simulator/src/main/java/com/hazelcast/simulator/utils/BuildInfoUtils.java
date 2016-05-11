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

import static com.hazelcast.simulator.utils.VersionUtils.parseVersionString;

public final class BuildInfoUtils {

    static final int DEFAULT_MINOR_VERSION = 5;

    private BuildInfoUtils() {
    }

    public static int getMinorVersion() {
        return getMinorVersion(getVersion());
    }

    static int getMinorVersion(String version) {
        if (version == null) {
            return DEFAULT_MINOR_VERSION;
        }
        String[] versions = parseVersionString(version);
        return Integer.parseInt(versions[1]);
    }

    public static boolean isMinVersion(String minVersion) {
        return isMinVersion(minVersion, getVersion());
    }

    static boolean isMinVersion(String minVersion, String version) {
        if (version == null) {
            return false;
        }
        return VersionUtils.isMinVersion(minVersion, getVersion());
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
}
