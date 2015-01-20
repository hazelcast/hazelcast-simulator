package com.hazelcast.stabilizer.provisioner;

import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.provisioner.git.GitSupport;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.hazelcast.stabilizer.Utils.exitWithError;
import static java.lang.String.format;

/**
 * Responsible for uploading the correct Hazelcast jars to the agents/workers.
 */
public class HazelcastJars {
    public static final String GIT_VERSION_PREFIX = "git=";
    public static final String MAVEN_VERSION_PREFIX = "maven=";

    private final static Logger log = Logger.getLogger(HazelcastJars.class);
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

    public void prepare(boolean eejars) {
        File tmpDir = new File(System.getProperty("java.io.tmpdir"));
        hazelcastJarsDir = new File(tmpDir, "hazelcastjars-" + UUID.randomUUID().toString());
        hazelcastJarsDir.mkdirs();

        log.info("Hazelcast version-spec: " + versionSpec);

        if (versionSpec.equals("outofthebox")) {
            //we don't need to do anything.
        } else if (versionSpec.equals("bringmyown")) {
            //we don't need to do anything
        } else if (versionSpec.startsWith(MAVEN_VERSION_PREFIX)) {
            String version = versionSpec.substring(MAVEN_VERSION_PREFIX.length());
            if (eejars) {
                mavenRetrieve("hazelcast-enterprise", version);
                mavenRetrieve("hazelcast-enterprise-client", version);
            } else {
                mavenRetrieve("hazelcast", version);
                mavenRetrieve("hazelcast-client", version);
                mavenRetrieve("hazelcast-wm", version);
            }
        } else if (versionSpec.startsWith(GIT_VERSION_PREFIX)) {
            if (eejars) {
                exitWithError(log, "Hazelcast Enterprise is currently not supported when HAZELCAST_VERSION_SPEC is set to GIT.");
            }
            String revision = versionSpec.substring(GIT_VERSION_PREFIX.length());
            gitRetrieve(revision);
        } else {
            log.fatal("Unrecognized version spec:" + versionSpec);
            System.exit(1);
        }
    }

    private void gitRetrieve(String revision) {
        File[] files = gitSupport.checkout(revision);
        Utils.copyFilesToDirectory(files, hazelcastJarsDir.getAbsoluteFile());
    }


    private void mavenRetrieve(String artifact, String version) {
        File userhome = new File(System.getProperty("user.home"));
        File repositoryDir = Utils.newFile(userhome, ".m2", "repository");
        File artifactFile = Utils.newFile(repositoryDir, "com", "hazelcast",
                artifact, version, format("%s-%s.jar", artifact, version));

        if (artifactFile.exists()) {
            log.info("Using artifact: " + artifactFile + " from local maven repository");
            bash.execute(format("cp %s %s", artifactFile.getAbsolutePath(), hazelcastJarsDir.getAbsolutePath()));
        } else {
            log.info("Artifact: " + artifactFile + " is not found in local maven repository, trying online one");

            String url;
            if (version.endsWith("-SNAPSHOT")) {
                String baseUrl = "https://oss.sonatype.org/content/repositories/snapshots";
                String mavenMetadataUrl = format("%s/com/hazelcast/%s/%s/maven-metadata.xml", baseUrl, artifact, version);
                log.debug("Loading: " + mavenMetadataUrl);
                String mavenMetadata = null;
                try {
                    mavenMetadata = Utils.getText(mavenMetadataUrl);
                } catch (FileNotFoundException e) {
                    log.fatal("Failed to load " + artifact + "-" + version + ", because :"
                            + mavenMetadataUrl + " was not found");
                    System.exit(1);
                } catch (IOException e) {
                    log.fatal("Could not load:" + mavenMetadataUrl);
                    System.exit(1);
                }

                log.debug(mavenMetadata);
                String timestamp = getTagValue(mavenMetadata, "timestamp");
                String buildnumber = getTagValue(mavenMetadata, "buildNumber");
                String shortVersion = version.replace("-SNAPSHOT", "");
                url = format("%s/com/hazelcast/%s/%s/%s-%s-%s-%s.jar",
                        baseUrl, artifact, version, artifact, shortVersion, timestamp, buildnumber);
            } else {
                String baseUrl = "http://repo1.maven.org/maven2";
                url = format("%s/com/hazelcast/%s/%s/%s-%s.jar", baseUrl, artifact, version, artifact, version);
            }

            bash.download(hazelcastJarsDir.getAbsolutePath(), url);
        }
    }

    private String getTagValue(String mavenMetadata, String tag) {
        final Pattern pattern = Pattern.compile("<" + tag + ">(.+?)</" + tag + ">");
        final Matcher matcher = pattern.matcher(mavenMetadata);

        if (!matcher.find()) {
            throw new RuntimeException("Could not find " + tag + " in:" + mavenMetadata);
        }

        return matcher.group(1);
    }
}
