package com.hazelcast.heartattacker;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

import static java.lang.String.format;

public class JavaInstallationsRepository {

    private final List<JavaInstallation> installationList = new LinkedList<JavaInstallation>();

    public JavaInstallation getAny() {
        if (installationList.isEmpty()) return null;
        return installationList.get(0);
    }

    public void load(File propertiesFile) {
        Properties properties = loadProperties(propertiesFile);

        Map<String, JavaInstallation> entries = new HashMap<String, JavaInstallation>();
        for (String key : properties.stringPropertyNames()) {
            String[] tokens = key.split("\\.");
            if (tokens.length != 2) {
                throw new RuntimeException(format("Invalid java-installations properties: property key [%s] should be of form x.y", key));
            }

            String value = properties.getProperty(key);

            String id = tokens[0];
            String property = tokens[1];
            JavaInstallation installation = entries.get(id);
            if (installation == null) {
                installation = new JavaInstallation();
                entries.put(id, installation);
            }

            if ("vendor".equalsIgnoreCase(property)) {
                installation.setVendor(value);
            } else if ("version".equalsIgnoreCase(property)) {
                installation.setVersion(value);
            } else if ("javaHome".equalsIgnoreCase(property)) {
                installation.setJavaHome(value);
            } else {
                throw new RuntimeException(format("Invalid java-installations properties: property key [%s] is not unrecognized", key));
            }
        }

        for (Map.Entry<String, JavaInstallation> entry : entries.entrySet()) {
            String id = entry.getKey();
            JavaInstallation installation = entry.getValue();

            if (installation.getVendor() == null) {
                throw new RuntimeException(format("Invalid java-installations properties: %s.vendor is missing", id));
            }

            if (installation.getVersion() == null) {
                throw new RuntimeException(format("Invalid java-installations properties: %s.version is missing", id));
            }

            if (installation.getJavaHome() == null) {
                throw new RuntimeException(format("Invalid java-installations properties: %s.javaHome is missing", id));
            }

            installationList.add(installation);
        }
    }

    private Properties loadProperties(File propertiesFile) {
        Properties properties = new Properties();

        try {
            final FileInputStream fis = new FileInputStream(propertiesFile);
            try {
                properties.load(fis);
            } finally {
                Utils.closeQuietly(fis);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return properties;
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
