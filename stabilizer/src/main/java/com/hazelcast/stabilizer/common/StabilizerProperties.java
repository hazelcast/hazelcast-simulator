package com.hazelcast.stabilizer.common;

import com.hazelcast.stabilizer.provisioner.HazelcastJars;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import static com.hazelcast.stabilizer.utils.CommonUtils.closeQuietly;
import static com.hazelcast.stabilizer.utils.CommonUtils.exitWithError;
import static com.hazelcast.stabilizer.utils.FileUtils.fileAsText;
import static com.hazelcast.stabilizer.utils.FileUtils.getStablizerHome;
import static com.hazelcast.stabilizer.utils.FileUtils.newFile;
import static java.lang.String.format;

/**
 * StabilizerProperties will always load the properties in the stabilizer_home/conf/stabilizer.properties
 * as defaults. If a stabilizer.properties is available in the working dir of if an explicit stabilizer.properties
 * is configured, it will override the properties from the default.
 */
public class StabilizerProperties {
    private final static Logger log = Logger.getLogger(StabilizerProperties.class);

    private final Properties properties = new Properties();
    private String forcedHazelcastVersionSpec;

    public StabilizerProperties() {
        File defaultPropsFile = newFile(getStablizerHome(), "conf", "stabilizer.properties");
        log.debug("Loading default stabilizer.properties from: " + defaultPropsFile.getAbsolutePath());
        load(defaultPropsFile);
    }

    public String getUser() {
        return get("USER", "stabilizer");
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
            log.info("Overriding Hazelcast version to GIT revision " + gitRevision);
        }
    }

    /**
     * Initialized the StabilizerProperties
     *
     * @param file the file to load the properties from. If the file is null, then first the stabilizer.properties
     *             in the working dir is checked.
     */
    public void init(File file) {
        if (file == null) {
            //if no file is explicitly given, we look in the working directory
            File fallbackPropsFile = new File("stabilizer.properties");
            if (fallbackPropsFile.exists()) {
                file = fallbackPropsFile;
            } else {
                log.warn(format("%s is not found, relying on defaults", fallbackPropsFile));
            }
        }

        if (file != null) {
            log.info(format("Loading stabilizer.properties: %s", file.getAbsolutePath()));
            load(file);
        }
    }

    private void load(File file) {
        if (!file.exists()) {
            exitWithError(log, "Could not find stabilizer.properties file: " + file.getAbsolutePath());
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
            exitWithError(log, format("Can't find %s file %s", property, value));
        }

        if (log.isDebugEnabled()) {
            log.debug("Loading " + property + " from file: " + file.getAbsolutePath());
        }
        return fileAsText(file).trim();
    }
}
