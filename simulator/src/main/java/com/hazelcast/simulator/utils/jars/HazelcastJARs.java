package com.hazelcast.simulator.utils.jars;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.provisioner.Bash;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.FileUtilsException;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.hazelcast.simulator.utils.FileUtils.copyFilesToDirectory;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.getText;
import static com.hazelcast.simulator.utils.FileUtils.newFile;
import static java.lang.String.format;

/**
 * Provides and uploads the correct Hazelcast JARs for a configured Hazelcast version.
 */
public class HazelcastJARs {

    public static final String GIT_VERSION_PREFIX = "git=";
    public static final String MAVEN_VERSION_PREFIX = "maven=";

    public static final String OUT_OF_THE_BOX = "outofthebox";
    public static final String BRING_MY_OWN = "bringmyown";

    private static final Logger LOGGER = Logger.getLogger(HazelcastJARs.class);

    private final Bash bash;
    private final GitSupport gitSupport;
    private final String versionSpec;

    private final File hazelcastJarsDir;

    HazelcastJARs(Bash bash, GitSupport gitSupport, String versionSpec) {
        this.bash = bash;
        this.gitSupport = gitSupport;
        this.versionSpec = versionSpec;

        File tmpDir = new File(System.getProperty("java.io.tmpdir"));
        this.hazelcastJarsDir = new File(tmpDir, "hazelcastjars-" + UUID.randomUUID().toString());
    }

    public static HazelcastJARs newInstance(Bash bash, SimulatorProperties properties) {
        return new HazelcastJARs(bash, GitSupport.newInstance(bash, properties), properties.getHazelcastVersionSpec());
    }

    public void prepare(boolean prepareEnterpriseJARs) {
        LOGGER.info("Hazelcast version-spec: " + versionSpec);
        if (versionSpec.equals(OUT_OF_THE_BOX) || versionSpec.equals(BRING_MY_OWN)) {
            // we don't need to do anything
            return;
        }

        ensureExistingDirectory(hazelcastJarsDir);

        if (versionSpec.startsWith(GIT_VERSION_PREFIX)) {
            if (prepareEnterpriseJARs) {
                throw new CommandLineExitException(
                        "Hazelcast Enterprise is currently not supported when HAZELCAST_VERSION_SPEC is set to Git.");
            }
            String revision = versionSpec.substring(GIT_VERSION_PREFIX.length());
            gitRetrieve(revision);
        } else if (versionSpec.startsWith(MAVEN_VERSION_PREFIX)) {
            String version = versionSpec.substring(MAVEN_VERSION_PREFIX.length());
            if (prepareEnterpriseJARs) {
                mavenRetrieve("hazelcast-enterprise", version);
                mavenRetrieve("hazelcast-enterprise-client", version);
            } else {
                mavenRetrieve("hazelcast", version);
                mavenRetrieve("hazelcast-client", version);
                mavenRetrieve("hazelcast-wm", version);
            }
        } else {
            throw new CommandLineExitException("Unrecognized version spec: " + versionSpec);
        }
    }

    public void upload(String ip, String simulatorHome) {
        if (OUT_OF_THE_BOX.equals(versionSpec)) {
            // upload Hazelcast JARs
            bash.uploadToAgentSimulatorDir(ip, simulatorHome + "/lib/hazelcast*", "lib");
        } else if (!BRING_MY_OWN.equals(versionSpec)) {
            // upload the actual Hazelcast JARs that are going to be used by the worker
            bash.uploadToAgentSimulatorDir(ip, hazelcastJarsDir.getAbsolutePath() + "/*.jar", "lib");
        }
    }

    // just for testing
    String getAbsolutePath() {
        return hazelcastJarsDir.getAbsolutePath();
    }

    private void gitRetrieve(String revision) {
        File[] files = gitSupport.checkout(revision);
        copyFilesToDirectory(files, hazelcastJarsDir.getAbsoluteFile());
    }

    private void mavenRetrieve(String artifact, String version) {
        File artifactFile = getArtifactFile(artifact, version);
        if (artifactFile.exists()) {
            LOGGER.info("Using artifact: " + artifactFile + " from local maven repository");
            bash.execute(format("cp %s %s", artifactFile.getAbsolutePath(), hazelcastJarsDir.getAbsolutePath()));
            return;
        }

        LOGGER.info("Artifact: " + artifactFile + " is not found in local maven repository, trying online one");
        String url;
        if (version.endsWith("-SNAPSHOT")) {
            url = getSnapshotUrl(artifact, version);
        } else {
            url = getReleaseUrl(artifact, version);
        }
        bash.download(hazelcastJarsDir.getAbsolutePath(), url);
    }

    File getArtifactFile(String artifact, String version) {
        File userHome = new File(System.getProperty("user.home"));
        File repositoryDir = newFile(userHome, ".m2", "repository");
        return newFile(repositoryDir, "com", "hazelcast", artifact, version, format("%s-%s.jar", artifact, version));
    }

    String getSnapshotUrl(String artifact, String version) {
        String baseUrl = "https://oss.sonatype.org/content/repositories/snapshots";
        String mavenMetadata = getMavenMetadata(artifact, version, baseUrl);
        LOGGER.debug(mavenMetadata);

        String shortVersion = version.replace("-SNAPSHOT", "");
        String timestamp = getTagValue(mavenMetadata, "timestamp");
        String buildNumber = getTagValue(mavenMetadata, "buildNumber");
        return format("%s/com/hazelcast/%s/%s/%s-%s-%s-%s.jar", baseUrl, artifact, version, artifact, shortVersion, timestamp,
                buildNumber);
    }

    String getReleaseUrl(String artifact, String version) {
        String baseUrl = "http://repo1.maven.org/maven2";
        return format("%s/com/hazelcast/%s/%s/%s-%s.jar", baseUrl, artifact, version, artifact, version);
    }

    String getMavenMetadata(String artifact, String version, String baseUrl) {
        String mavenMetadataUrl = format("%s/com/hazelcast/%s/%s/maven-metadata.xml", baseUrl, artifact, version);
        LOGGER.debug("Loading: " + mavenMetadataUrl);
        try {
            return getText(mavenMetadataUrl);
        } catch (FileUtilsException e) {
            throw new CommandLineExitException("Could not load " + mavenMetadataUrl, e);
        }
    }

    String getTagValue(String mavenMetadata, String tag) {
        Pattern pattern = Pattern.compile('<' + tag + ">(.+?)</" + tag + '>');
        Matcher matcher = pattern.matcher(mavenMetadata);
        if (!matcher.find()) {
            throw new CommandLineExitException("Could not find " + tag + " in " + mavenMetadata);
        }

        return matcher.group(1);
    }
}
