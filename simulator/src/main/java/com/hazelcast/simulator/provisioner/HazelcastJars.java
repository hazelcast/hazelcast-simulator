package com.hazelcast.simulator.provisioner;

import com.hazelcast.simulator.provisioner.git.GitSupport;
import com.hazelcast.simulator.utils.CommonUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.hazelcast.simulator.utils.FileUtils.copyFilesToDirectory;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.getText;
import static com.hazelcast.simulator.utils.FileUtils.newFile;
import static java.lang.String.format;

/**
 * Responsible for uploading the correct Hazelcast JARs to the agents/workers.
 */
public class HazelcastJars {

    public static final String GIT_VERSION_PREFIX = "git=";
    public static final String MAVEN_VERSION_PREFIX = "maven=";

    private static final Logger LOGGER = Logger.getLogger(HazelcastJars.class);

    private final Bash bash;
    private final GitSupport gitSupport;
    private final String versionSpec;

    private File hazelcastJarsDir;

    public HazelcastJars(Bash bash, GitSupport gitSupport, String versionSpec) {
        this.bash = bash;
        this.versionSpec = versionSpec;
        this.gitSupport = gitSupport;
    }

    public String getAbsolutePath() {
        return hazelcastJarsDir.getAbsolutePath();
    }

    public void prepare(boolean prepareEnterpriseJARs) {
        File tmpDir = new File(System.getProperty("java.io.tmpdir"));
        hazelcastJarsDir = new File(tmpDir, "hazelcastjars-" + UUID.randomUUID().toString());
        ensureExistingDirectory(hazelcastJarsDir);

        LOGGER.info("Hazelcast version-spec: " + versionSpec);

        if (versionSpec.equals("outofthebox") || versionSpec.equals("bringmyown")) {
            // we don't need to do anything
            return;
        }

        if (versionSpec.startsWith(MAVEN_VERSION_PREFIX)) {
            String version = versionSpec.substring(MAVEN_VERSION_PREFIX.length());
            if (prepareEnterpriseJARs) {
                mavenRetrieve("hazelcast-enterprise", version);
                mavenRetrieve("hazelcast-enterprise-client", version);
            } else {
                mavenRetrieve("hazelcast", version);
                mavenRetrieve("hazelcast-client", version);
                mavenRetrieve("hazelcast-wm", version);
            }
        } else if (versionSpec.startsWith(GIT_VERSION_PREFIX)) {
            if (prepareEnterpriseJARs) {
                CommonUtils.exitWithError(LOGGER,
                        "Hazelcast Enterprise is currently not supported when HAZELCAST_VERSION_SPEC is set to GIT.");
            }
            String revision = versionSpec.substring(GIT_VERSION_PREFIX.length());
            gitRetrieve(revision);
        } else {
            LOGGER.fatal("Unrecognized version spec: " + versionSpec);
            System.exit(1);
        }
    }

    private void gitRetrieve(String revision) {
        File[] files = gitSupport.checkout(revision);
        copyFilesToDirectory(files, hazelcastJarsDir.getAbsoluteFile());
    }

    private void mavenRetrieve(String artifact, String version) {
        File userHome = new File(System.getProperty("user.home"));
        File repositoryDir = newFile(userHome, ".m2", "repository");
        File artifactFile = newFile(repositoryDir, "com", "hazelcast", artifact, version, format("%s-%s.jar", artifact, version));

        if (artifactFile.exists()) {
            LOGGER.info("Using artifact: " + artifactFile + " from local maven repository");
            bash.execute(format("cp %s %s", artifactFile.getAbsolutePath(), hazelcastJarsDir.getAbsolutePath()));
        } else {
            LOGGER.info("Artifact: " + artifactFile + " is not found in local maven repository, trying online one");

            String url;
            if (version.endsWith("-SNAPSHOT")) {
                String baseUrl = "https://oss.sonatype.org/content/repositories/snapshots";
                String mavenMetadataUrl = format("%s/com/hazelcast/%s/%s/maven-metadata.xml", baseUrl, artifact, version);
                LOGGER.debug("Loading: " + mavenMetadataUrl);
                String mavenMetadata = null;
                try {
                    mavenMetadata = getText(mavenMetadataUrl);
                } catch (FileNotFoundException e) {
                    LOGGER.fatal("Failed to load " + artifact + "-" + version
                            + ", because " + mavenMetadataUrl + " was not found");
                    System.exit(1);
                } catch (IOException e) {
                    LOGGER.fatal("Could not load " + mavenMetadataUrl);
                    System.exit(1);
                }

                LOGGER.debug(mavenMetadata);
                String timestamp = getTagValue(mavenMetadata, "timestamp");
                String buildNumber = getTagValue(mavenMetadata, "buildNumber");
                String shortVersion = version.replace("-SNAPSHOT", "");
                url = format("%s/com/hazelcast/%s/%s/%s-%s-%s-%s.jar",
                        baseUrl, artifact, version, artifact, shortVersion, timestamp, buildNumber);
            } else {
                String baseUrl = "http://repo1.maven.org/maven2";
                url = format("%s/com/hazelcast/%s/%s/%s-%s.jar", baseUrl, artifact, version, artifact, version);
            }

            bash.download(hazelcastJarsDir.getAbsolutePath(), url);
        }
    }

    private String getTagValue(String mavenMetadata, String tag) {
        final Pattern pattern = Pattern.compile('<' + tag + ">(.+?)</" + tag + '>');
        final Matcher matcher = pattern.matcher(mavenMetadata);

        if (!matcher.find()) {
            throw new RuntimeException("Could not find " + tag + " in " + mavenMetadata);
        }

        return matcher.group(1);
    }
}
