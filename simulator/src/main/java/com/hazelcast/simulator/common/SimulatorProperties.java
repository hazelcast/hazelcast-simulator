package com.hazelcast.simulator.common;

import com.hazelcast.simulator.provisioner.HazelcastJars;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.FileUtils.newFile;
import static java.lang.String.format;

/**
 * SimulatorProperties will always load the properties in the simulator_home/conf/simulator.properties
 * as defaults. If a simulator.properties is available in the working dir of if an explicit simulator.properties
 * is configured, it will override the properties from the default.
 */
public class SimulatorProperties {
    private static final Logger LOGGER = Logger.getLogger(SimulatorProperties.class);

    private final Properties properties = new Properties();
    private String forcedHazelcastVersionSpec;

    public SimulatorProperties() {
        File defaultPropsFile = newFile(getSimulatorHome(), "conf", "simulator.properties");
        LOGGER.debug("Loading default simulator.properties from: " + defaultPropsFile.getAbsolutePath());
        load(defaultPropsFile);
    }

    public String getUser() {
        return get("USER", "simulator");
    }

    public boolean isEc2() {
        return "aws-ec2".equals(get("CLOUD_PROVIDER"));
    }

    public String getHazelcastVersionSpec() {
        if (forcedHazelcastVersionSpec == null) {
            return get("HAZELCAST_VERSION_SPEC", "outofthebox");
        } else {
            return forcedHazelcastVersionSpec;
        }
    }

    public void forceGit(String gitRevision) {
        if (gitRevision != null && !gitRevision.isEmpty()) {
            forcedHazelcastVersionSpec = HazelcastJars.GIT_VERSION_PREFIX + gitRevision;
            LOGGER.info("Overriding Hazelcast version to GIT revision " + gitRevision);
        }
    }

    /**
     * Initialized the SimulatorProperties
     *
     * @param file the file to load the properties from. If the file is null, then first the simulator.properties
     *             in the working dir is checked.
     */
    public void init(File file) {
        if (file == null) {
            //if no file is explicitly given, we look in the working directory
            File fallbackPropsFile = new File("simulator.properties");
            if (fallbackPropsFile.exists()) {
                file = fallbackPropsFile;
            } else {
                LOGGER.warn(format("%s is not found, relying on defaults", fallbackPropsFile));
            }
        }

        if (file != null) {
            LOGGER.info(format("Loading simulator.properties: %s", file.getAbsolutePath()));
            load(file);
        }
    }

    private void load(File file) {
        if (!file.exists()) {
            exitWithError(LOGGER, "Could not find simulator.properties file: " + file.getAbsolutePath());
            return;
        }

        FileInputStream inputStream = null;
        try {
            inputStream = new FileInputStream(file);
            try {
                properties.load(inputStream);
            } catch (IOException e) {
                closeQuietly(inputStream);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            closeQuietly(inputStream);
        }
    }

    public String get(String name) {
        String value = (String) properties.get(name);

        if ("CLOUD_IDENTITY".equals(name)) {
            value = load("CLOUD_IDENTITY", value);
        } else if ("CLOUD_CREDENTIAL".equals(name)) {
            value = load("CLOUD_CREDENTIAL", value);
        }

        return fixValue(name, value);
    }

    public String get(String name, String defaultValue) {
        String value = System.getProperty(name);
        if (value != null) {
            return value;
        }

        value = (String) properties.get(name);
        if (value == null) {
            value = defaultValue;
        }

        return fixValue(name, value);
    }

    private String fixValue(String name, String value) {
        if (value == null) {
            return null;
        }

        if ("GROUP_NAME".equals(name)) {
            String username = System.getProperty("user.name").toLowerCase();

            StringBuffer sb = new StringBuffer();
            for (char c : username.toCharArray()) {
                if (Character.isLetter(c)) {
                    sb.append(c);
                } else if (Character.isDigit(c)) {
                    sb.append(c);
                }
            }

            value = value.replace("${username}", username);
        }

        return value;
    }

    private String load(String property, String value) {
        File file = newFile(value);
        if (!file.exists()) {
            exitWithError(LOGGER, format("Can't find %s file %s", property, value));
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Loading " + property + " from file: " + file.getAbsolutePath());
        }
        return fileAsText(file).trim();
    }
}
