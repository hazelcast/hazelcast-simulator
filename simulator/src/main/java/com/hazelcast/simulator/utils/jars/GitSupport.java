/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import org.apache.log4j.Logger;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.ResetCommand;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.errors.RepositoryNotFoundException;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.StoredConfig;
import org.eclipse.jgit.transport.RefSpec;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.simulator.utils.FileUtils.USER_HOME;
import static com.hazelcast.simulator.utils.FileUtils.copyFilesToDirectory;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.newFile;

class GitSupport {

    private static final String HAZELCAST_MAIN_REPO_URL = "https://github.com/hazelcast/hazelcast.git";
    private static final String CONFIG_REMOTE = "remote";
    private static final String CONFIG_URL = "url";

    private static final Logger LOGGER = Logger.getLogger(GitSupport.class);

    private final BuildSupport buildSupport;
    private final File baseDir;
    private final Set<GitRepository> customRepositories;

    GitSupport(BuildSupport buildSupport, String customRepositories, String basePath) {
        this.buildSupport = buildSupport;
        this.baseDir = getBaseDir(basePath);
        this.customRepositories = getCustomRepositories(customRepositories);
    }

    static GitSupport newInstance(Bash bash, SimulatorProperties properties) {
        String mvnExec = properties.get("MVN_EXECUTABLE");
        String customGitRepositories = properties.get("GIT_CUSTOM_REPOSITORIES");
        String gitBuildDirectory = properties.get("GIT_BUILD_DIR");

        BuildSupport buildSupport = new BuildSupport(bash, new HazelcastJARFinder(), mvnExec);
        return new GitSupport(buildSupport, customGitRepositories, gitBuildDirectory);
    }

    File[] checkout(String revision) {
        File srcDirectory = getOrCreateSourceDirectory();
        String fullSha1 = fetchSources(srcDirectory, revision);

        File buildCache = getCacheDirectory(fullSha1);
        if (!buildCache.exists()) {
            buildAndCache(srcDirectory, buildCache);
        } else {
            LOGGER.info("Hazelcast JARs found in build-cache " + buildCache.getAbsolutePath());
        }
        return buildCache.listFiles();
    }

    private Set<GitRepository> getCustomRepositories(String customRepositories) {
        if (customRepositories == null || customRepositories.isEmpty()) {
            return Collections.emptySet();
        }
        String[] repositoriesArray = customRepositories.split(",");
        Set<GitRepository> repositories = new HashSet<GitRepository>(repositoriesArray.length);
        for (String repository : repositoriesArray) {
            String normalized = repository.trim();
            if (!normalized.isEmpty()) {
                GitRepository repo = GitRepository.fromString(normalized);
                repositories.add(repo);
            }
        }
        return Collections.unmodifiableSet(repositories);
    }

    private File getDefaultBaseDir() {
        return newFile(USER_HOME, ".hazelcast-build");
    }

    private void syncRemoteRepositories(Git git) throws IOException {
        StoredConfig config = git.getRepository().getConfig();
        Set<GitRepository> customRepositoriesCopy = new HashSet<GitRepository>(customRepositories);

        Set<String> existingRemoteRepoNames = config.getSubsections(CONFIG_REMOTE);
        for (String remoteName : existingRemoteRepoNames) {
            String url = config.getString(CONFIG_REMOTE, remoteName, CONFIG_URL);
            boolean isConfigured = customRepositoriesCopy.remove(new GitRepository(remoteName, url));
            if (!isConfigured && isCustomRepository(remoteName)) {
                removeRepository(config, remoteName);
            }
        }

        for (GitRepository repository : customRepositoriesCopy) {
            addRepository(config, repository);
        }
        config.save();
    }

    private void addRepository(StoredConfig config, GitRepository repository) {
        String url = repository.getUrl();
        String name = repository.getName();
        LOGGER.info("Adding a new custom repository " + url);
        config.setString(CONFIG_REMOTE, name, CONFIG_URL, url);
        RefSpec refSpec = new RefSpec()
                .setForceUpdate(true)
                .setSourceDestination(Constants.R_HEADS + '*', Constants.R_REMOTES + name + "/*");
        config.setString(CONFIG_REMOTE, name, "fetch", refSpec.toString());
    }

    private void removeRepository(StoredConfig config, String remoteName) {
        config.unsetSection(CONFIG_REMOTE, remoteName);
    }

    private boolean isCustomRepository(String remoteName) {
        return !remoteName.equals("origin");
    }

    private String fetchSources(File path, String revision) {
        Git git = null;
        String fullSha1 = null;
        try {
            git = cloneIfNecessary(path);
            syncRemoteRepositories(git);
            fetchAllRepositories(git);
            fullSha1 = checkoutRevision(git, revision);
        } catch (GitAPIException e) {
            throw new CommandLineExitException("Error while fetching sources from Git", e);
        } catch (IOException e) {
            throw new CommandLineExitException("Error while fetching sources from Git", e);
        } finally {
            if (git != null) {
                git.close();
            }
        }
        return fullSha1;
    }

    private File getBaseDir(String basePath) {
        File tmpBaseDir;
        if (basePath == null) {
            tmpBaseDir = getDefaultBaseDir();
            if (tmpBaseDir.exists()) {
                if (!tmpBaseDir.isDirectory()) {
                    throw new CommandLineExitException(
                            "Default directory for building Hazelcast from Git is " + tmpBaseDir.getAbsolutePath()
                                    + ". This path already exists, but it isn't a directory."
                                    + " Please configure the directory explicitly via 'simulator.properties'"
                                    + " or remove the existing path.");
                } else if (!tmpBaseDir.canWrite()) {
                    throw new CommandLineExitException(
                            "Default directory for building Hazelcast from Git is " + tmpBaseDir.getAbsolutePath()
                                    + ". This path already exists, but it isn't writable."
                                    + " Please configure the directory explicitly via 'simulator.properties'"
                                    + " or check access rights.");
                }
            }
        } else {
            tmpBaseDir = new File(basePath);
            if (tmpBaseDir.exists()) {
                if (!tmpBaseDir.isDirectory()) {
                    throw new CommandLineExitException("Directory for building Hazelcast from Git is "
                            + tmpBaseDir.getAbsolutePath() + ". This path already exists, but it isn't a directory.");
                } else if (!tmpBaseDir.canWrite()) {
                    throw new CommandLineExitException("Directory for building Hazelcast from Git is "
                            + tmpBaseDir.getAbsolutePath() + ". This path already exists, but it isn't writable.");
                }
            }
        }
        if (!tmpBaseDir.exists()) {
            ensureExistingDirectory(tmpBaseDir);
            if (!tmpBaseDir.exists()) {
                throw new CommandLineExitException("Cannot create a directory for building Hazelcast from Git."
                        + " Directory is set to " + tmpBaseDir.getAbsolutePath() + ". Please check access rights.");
            }
        }
        return tmpBaseDir;
    }

    private File getCacheDirectory(String fullSha1) {
        return newFile(baseDir, "build-cache", fullSha1);
    }

    private File getOrCreateSourceDirectory() {
        File src = newFile(baseDir, "src");
        ensureExistingDirectory(src);
        return src;
    }

    private void fetchAllRepositories(Git git) throws GitAPIException {
        Repository repository = git.getRepository();
        Set<String> remotes = repository.getRemoteNames();
        for (String remoteRepository : remotes) {
            git.fetch().setRemote(remoteRepository).call();

        }
    }

    private void buildAndCache(File src, File buildCache) {
        File[] files = buildSupport.build(src);
        ensureExistingDirectory(buildCache);
        copyFilesToDirectory(files, buildCache);
    }

    private Git cloneIfNecessary(File src) throws GitAPIException, IOException {
        Git git;
        if (!isValidLocalRepository(src)) {
            LOGGER.info("Cloning Hazelcast Git repository to " + src.getAbsolutePath() + ". This might take a while...");
            git = Git.cloneRepository().setURI(HAZELCAST_MAIN_REPO_URL).setDirectory(src).call();
        } else {
            git = Git.open(src);
        }
        return git;
    }

    private String checkoutRevision(Git git, String revision) throws GitAPIException, IOException {
        resetHard(git);
        git.checkout().setForce(true).setName(revision).setStartPoint(revision).call();
        return git.getRepository().resolve(revision).toObjectId().name();
    }

    private void resetHard(Git git) throws GitAPIException {
        git.reset().setMode(ResetCommand.ResetType.HARD).call();
    }

    private boolean isValidLocalRepository(File repository) {
        boolean result = false;
        Git git = null;
        try {
            git = Git.open(repository);
            result = true;
        } catch (RepositoryNotFoundException e) {
            result = false;
        } catch (IOException e) {
            result = false;
        } finally {
            if (git != null) {
                git.close();
            }
        }
        return result;
    }
}
