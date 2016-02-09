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
package com.hazelcast.simulator.agent;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static java.lang.String.format;

class JavaInstallationsRepository {

    private final List<JavaInstallation> installationList = new LinkedList<JavaInstallation>();

    public JavaInstallation getAny() {
        if (installationList.isEmpty()) {
            return null;
        }
        return installationList.get(0);
    }

    public void load(File propertiesFile) {
        Map<String, JavaInstallation> javaInstallations = new HashMap<String, JavaInstallation>();

        Properties properties = loadProperties(propertiesFile);
        parseInstallations(properties, javaInstallations);

        checkParsedInstallations(javaInstallations);
    }

    private Properties loadProperties(File propertiesFile) {
        Properties properties = new Properties();
        try {
            FileInputStream fis = new FileInputStream(propertiesFile);
            try {
                properties.load(fis);
            } finally {
                closeQuietly(fis);
            }
        } catch (IOException e) {
            throw new JavaInstallationException(e);
        }
        return properties;
    }

    private void parseInstallations(Properties properties, Map<String, JavaInstallation> javaInstallations) {
        for (String key : properties.stringPropertyNames()) {
            String[] tokens = key.split("\\.");
            if (tokens.length != 2) {
                throw new JavaInstallationException(
                        format("Invalid java-installations properties: property key [%s] should be of form x.y", key));
            }

            String id = tokens[0];
            String property = tokens[1];

            JavaInstallation installation = javaInstallations.get(id);
            if (installation == null) {
                installation = new JavaInstallation();
                javaInstallations.put(id, installation);
            }

            String value = properties.getProperty(key);
            if ("vendor".equalsIgnoreCase(property)) {
                installation.setVendor(value);
            } else if ("version".equalsIgnoreCase(property)) {
                installation.setVersion(value);
            } else if ("javaHome".equalsIgnoreCase(property)) {
                installation.setJavaHome(value);
            } else {
                throw new JavaInstallationException(
                        format("Invalid java-installations properties: property key [%s] is not unrecognized", key));
            }
        }
    }

    private void checkParsedInstallations(Map<String, JavaInstallation> javaInstallations) {
        for (Map.Entry<String, JavaInstallation> entry : javaInstallations.entrySet()) {
            String id = entry.getKey();
            JavaInstallation installation = entry.getValue();

            if (installation.getVendor() == null) {
                throw new JavaInstallationException(format("Invalid java-installations properties: %s.vendor is missing", id));
            }
            if (installation.getVersion() == null) {
                throw new JavaInstallationException(format("Invalid java-installations properties: %s.version is missing", id));
            }
            if (installation.getJavaHome() == null) {
                throw new JavaInstallationException(format("Invalid java-installations properties: %s.javaHome is missing", id));
            }

            installationList.add(installation);
        }
    }

    public JavaInstallation get(String vendor, String version) {
        for (JavaInstallation installation : installationList) {
            if (installation.getVendor().equals(vendor) && installation.getVersion().equals(version)) {
                return installation;
            }
        }
        return null;
    }
}
