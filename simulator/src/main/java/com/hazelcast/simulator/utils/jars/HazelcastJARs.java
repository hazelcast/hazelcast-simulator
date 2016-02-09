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
package com.hazelcast.simulator.utils.jars;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.utils.Bash;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.FileUtilsException;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.FileUtils.USER_HOME;
import static com.hazelcast.simulator.utils.FileUtils.copyFilesToDirectory;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.getText;
import static com.hazelcast.simulator.utils.FileUtils.newFile;
import static com.hazelcast.simulator.utils.UuidUtil.newSecureUuidString;
import static java.lang.String.format;

/**
 * Provides and uploads the correct Hazelcast JARs for a configured Hazelcast version.
 */
public class HazelcastJARs {

    public static final String GIT_VERSION_PREFIX = "git=";
    public static final String MAVEN_VERSION_PREFIX = "maven=";

    public static final String OUT_OF_THE_BOX = "outofthebox";
    public static final String BRING_MY_OWN = "bringmyown";

    private static final String SONATYPE_SNAPSHOT_REPOSITORY = "https://oss.sonatype.org/content/repositories/snapshots";
    private static final String MAVEN_RELEASE_REPOSITORY = "https://repo1.maven.org/maven2";

    private static final String CLOUDBEES_SNAPSHOT_REPOSITORY = "https://repository-hazelcast-l337.forge.cloudbees.com/snapshot";
    private static final String CLOUDBEES_RELEASE_REPOSITORY = "https://repository-hazelcast-l337.forge.cloudbees.com/release";

    private static final Logger LOGGER = Logger.getLogger(HazelcastJARs.class);

    private final Map<String, File> versionSpecDirs = new HashMap<String, File>();

    private final Bash bash;
    private final GitSupport gitSupport;

    HazelcastJARs(Bash bash, GitSupport gitSupport) {
        this.bash = bash;
        this.gitSupport = gitSupport;
    }

    public static HazelcastJARs newInstance(Bash bash, SimulatorProperties properties, Set<String> versionSpecs) {
        HazelcastJARs hazelcastJARs = new HazelcastJARs(bash, GitSupport.newInstance(bash, properties));
        for (String versionSpec : versionSpecs) {
            hazelcastJARs.addVersionSpec(versionSpec);
        }
        return hazelcastJARs;
    }

    public static String directoryForVersionSpec(String versionSpec) {
        if (BRING_MY_OWN.equals(versionSpec)) {
            return null;
        }
        if (OUT_OF_THE_BOX.equals(versionSpec)) {
            return "outofthebox";
        }
        return versionSpec.replace('=', '-');
    }

    public static boolean isPrepareRequired(String versionSpec) {
        return (!OUT_OF_THE_BOX.equals(versionSpec) && !BRING_MY_OWN.equals(versionSpec));
    }

    public void prepare(boolean prepareEnterpriseJARs) {
        for (Map.Entry<String, File> versionSpecEntry : versionSpecDirs.entrySet()) {
            prepare(versionSpecEntry.getKey(), versionSpecEntry.getValue(), prepareEnterpriseJARs);
        }
    }

    public void upload(String ip, String simulatorHome) {
        upload(ip, simulatorHome, versionSpecDirs.keySet());
    }

    public void upload(String ip, String simulatorHome, Set<String> versionSpecs) {
        for (String versionSpec : versionSpecs) {
            // create target directory
            String versionDir = directoryForVersionSpec(versionSpec);
            if (versionDir != null) {
                bash.ssh(ip, format("mkdir -p hazelcast-simulator-%s/hz-lib/%s", getSimulatorVersion(), versionDir));
            }

            if (OUT_OF_THE_BOX.equals(versionSpec)) {
                // upload Hazelcast JARs
                bash.uploadToRemoteSimulatorDir(ip, simulatorHome + "/lib/hazelcast*", "hz-lib/outofthebox");
            } else if (!BRING_MY_OWN.equals(versionSpec)) {
                // upload the actual Hazelcast JARs that are going to be used by the worker
                File versionSpecDir = versionSpecDirs.get(versionSpec);
                bash.uploadToRemoteSimulatorDir(ip, versionSpecDir + "/*.jar", "hz-lib/" + versionDir);
            }
        }
    }

    public void shutdown() {
        for (File versionDir : versionSpecDirs.values()) {
            deleteQuiet(versionDir);
        }
    }

    void addVersionSpec(String versionSpec) {
        File tmpDir = new File(System.getProperty("java.io.tmpdir"));
        versionSpecDirs.put(versionSpec, new File(tmpDir, "hazelcastjars-" + newSecureUuidString()).getAbsoluteFile());
    }

    // just for testing
    Set<String> getVersionSpecs() {
        return versionSpecDirs.keySet();
    }

    // just for testing
    String getAbsolutePath(String versionSpec) {
        return versionSpecDirs.get(versionSpec).getAbsolutePath();
    }

    private void prepare(String versionSpec, File targetDir, boolean prepareEnterpriseJARs) {
        LOGGER.info("Hazelcast version-spec: " + versionSpec);
        if (!isPrepareRequired(versionSpec)) {
            if (prepareEnterpriseJARs) {
                LOGGER.warn("You have to deploy the Hazelcast Enterprise JARs on your own!");
            }
            return;
        }

        ensureExistingDirectory(targetDir);

        if (versionSpec.startsWith(GIT_VERSION_PREFIX)) {
            if (prepareEnterpriseJARs) {
                throw new CommandLineExitException(
                        "Hazelcast Enterprise is currently not supported when HAZELCAST_VERSION_SPEC is set to Git.");
            }
            String revision = versionSpec.substring(GIT_VERSION_PREFIX.length());
            gitRetrieve(targetDir, revision);
        } else if (versionSpec.startsWith(MAVEN_VERSION_PREFIX)) {
            String version = versionSpec.substring(MAVEN_VERSION_PREFIX.length());
            if (prepareEnterpriseJARs) {
                mavenRetrieve(targetDir, "hazelcast-enterprise", version, true);
                mavenRetrieve(targetDir, "hazelcast-enterprise-client", version, true);
            } else {
                mavenRetrieve(targetDir, "hazelcast", version, false);
                mavenRetrieve(targetDir, "hazelcast-client", version, false);
                mavenRetrieve(targetDir, "hazelcast-wm", version, false);
            }
        } else {
            throw new CommandLineExitException("Unrecognized version spec: " + versionSpecDirs);
        }
    }

    private void gitRetrieve(File targetDir, String revision) {
        File[] files = gitSupport.checkout(revision);
        copyFilesToDirectory(files, targetDir);
    }

    private void mavenRetrieve(File targetDir, String artifact, String version, boolean prepareEnterpriseJARs) {
        File artifactFile = getArtifactFile(artifact, version);
        if (artifactFile.exists()) {
            LOGGER.info(format("Using artifact %s from local Maven repository", artifactFile.getName()));
            bash.execute(format("cp %s %s", artifactFile.getAbsolutePath(), targetDir));
            return;
        }

        LOGGER.info(format("Artifact %s is not found in local Maven repository %s, trying to fetch from remote repository...",
                artifactFile.getName(), getRepositoryDir()));
        String url;
        if (version.endsWith("-SNAPSHOT")) {
            url = getSnapshotUrl(artifact, version, prepareEnterpriseJARs);
        } else {
            url = getReleaseUrl(artifact, version, prepareEnterpriseJARs);
        }
        bash.download(url, targetDir.getAbsolutePath());
    }

    private File getRepositoryDir() {
        return newFile(USER_HOME, ".m2", "repository");
    }

    File getArtifactFile(String artifact, String version) {
        return newFile(getRepositoryDir(), "com", "hazelcast", artifact, version, format("%s-%s.jar", artifact, version));
    }

    String getSnapshotUrl(String artifact, String version, boolean prepareEnterpriseJARs) {
        String baseUrl = (prepareEnterpriseJARs ? CLOUDBEES_SNAPSHOT_REPOSITORY : SONATYPE_SNAPSHOT_REPOSITORY);
        String mavenMetadata = getMavenMetadata(artifact, version, baseUrl);
        LOGGER.debug(mavenMetadata);

        String shortVersion = version.replace("-SNAPSHOT", "");
        String timestamp = getTagValue(mavenMetadata, "timestamp");
        String buildNumber = getTagValue(mavenMetadata, "buildNumber");
        return format("%s/com/hazelcast/%s/%s/%s-%s-%s-%s.jar", baseUrl, artifact, version, artifact, shortVersion, timestamp,
                buildNumber);
    }

    String getReleaseUrl(String artifact, String version, boolean prepareEnterpriseJARs) {
        String baseUrl = (prepareEnterpriseJARs ? CLOUDBEES_RELEASE_REPOSITORY : MAVEN_RELEASE_REPOSITORY);
        return format("%s/com/hazelcast/%s/%s/%s-%s.jar", baseUrl, artifact, version, artifact, version);
    }

    String getMavenMetadata(String artifact, String version, String baseUrl) {
        String mavenMetadataUrl = format("%s/com/hazelcast/%s/%s/maven-metadata.xml", baseUrl, artifact, version);
        LOGGER.debug("Loading " + mavenMetadataUrl);
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
