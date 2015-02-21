package com.hazelcast.stabilizer.provisioner.git;

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

import static com.hazelcast.stabilizer.utils.CommonUtils.exitWithError;
import static com.hazelcast.stabilizer.utils.FileUtils.copyFilesToDirectory;
import static com.hazelcast.stabilizer.utils.FileUtils.newFile;

public class GitSupport {
    public static final String CUSTOM_REPOSITORY_PREFIX = "custom";

    private static final String HAZELCAST_MAIN_REPO_URL = "https://github.com/hazelcast/hazelcast.git";
    private static final Logger log = Logger.getLogger(GitSupport.class);

    private final BuildSupport buildSupport;
    private final File baseDir;
    private final Set<GitRepository> customRepositories;

    public GitSupport(BuildSupport buildSupport) {
        this(buildSupport, null);
    }

    public GitSupport(BuildSupport buildSupport, String customRepositories) {
        this(buildSupport, customRepositories, null);
    }

    public GitSupport(BuildSupport buildSupport, String customRepositories, String basePath) {
        this.buildSupport = buildSupport;
        this.baseDir = getBaseDir(basePath);
        this.customRepositories = getCustomRepositories(customRepositories);
    }

    public File[] checkout(String revision) {
        File srcDirectory = getOrCreateSourceDirectory();
        String fullSha1 = fetchSources(srcDirectory, revision);

        File buildCache = getCacheDirectory(fullSha1);
        if (!buildCache.exists()) {
            buildAndCache(srcDirectory, buildCache);
        } else {
            log.info("Hazelcast JARs found in build-cache " + buildCache.getAbsolutePath());
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
        File homeDir = new File(System.getProperty("user.home"));
        return newFile(homeDir, ".hazelcast-build");
    }

    private void syncRemoteRepositories(Git git) throws IOException {
        StoredConfig config = git.getRepository().getConfig();
        Set<GitRepository> customRepositoriesCopy = new HashSet<GitRepository>(customRepositories);

        Set<String> existingRemoteRepoNames = config.getSubsections("remote");
        for (String remoteName : existingRemoteRepoNames) {
            String url = config.getString("remote", remoteName, "url");
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
        log.info("Adding a new custom repository " + url);
        config.setString("remote", name, "url", url);
        RefSpec refSpec = new RefSpec()
                .setForceUpdate(true)
                .setSourceDestination(Constants.R_HEADS + "*", Constants.R_REMOTES + name + "/*");
        config.setString("remote", name, "fetch", refSpec.toString());
    }

    private void removeRepository(StoredConfig config, String remoteName) {
        config.unsetSection("remote", remoteName);
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
            exitWithError(log, "Error while fetching sources from GIT", e);
        } catch (IOException e) {
            exitWithError(log, "Error while fetching sources from GIT", e);
        } finally {
            if (git != null) {
                git.close();
            }
        }
        return fullSha1;
    }

    private File getBaseDir(String basePath) {
        File baseDir;
        if (basePath == null) {
            baseDir = getDefaultBaseDir();
            if (baseDir.exists()) {
                if (!baseDir.isDirectory()) {
                    exitWithError(log, "Default directory for building Hazelcast from GIT is "
                            + baseDir.getAbsolutePath() + ". This path already exists, but it isn't a directory. " +
                            "Please configure the directory explicitly via stabilizer.properties or remove the existing path.");
                } else if (!baseDir.canWrite()) {
                    exitWithError(log, "Default directory for building Hazelcast from GIT is "
                            + baseDir.getAbsolutePath() + ". This path already exists, but it isn't writable. " +
                            "Please configure the directory explicitly via stabilizer.properties or check access rights.");
                }
            }
        } else {
            baseDir = new File(basePath);
            if (baseDir.exists()) {
                if (!baseDir.isDirectory()) {
                    exitWithError(log, "Directory for building Hazelcast from GIT is "
                            + baseDir.getAbsolutePath() + ". This path already exists, but it isn't a directory. ");
                } else if (!baseDir.canWrite()) {
                    exitWithError(log, "Directory for building Hazelcast from GIT is "
                            + baseDir.getAbsolutePath() + ". This path already exists, but it isn't writable. ");
                }
            }
        }
        if (!baseDir.exists()) {
            baseDir.mkdirs();
            if (!baseDir.exists()) {
                exitWithError(log, "Cannot create a directory for building Hazelcast form GIT. Directory is set to "
                        + baseDir.getAbsolutePath() + ". Please check access rights.");
            }
        }
        return baseDir;
    }

    private File getCacheDirectory(String fullSha1) {
        return newFile(baseDir, "build-cache", fullSha1);
    }

    private File getOrCreateSourceDirectory() {
        File src = newFile(baseDir, "src");
        src.mkdirs();
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
        buildCache.mkdirs();
        copyFilesToDirectory(files, buildCache);
    }

    private Git cloneIfNecessary(File src) throws GitAPIException, IOException {
        Git git;
        if (!isValidLocalRepository(src)) {
            log.info("Cloning Hazelcast GIT repository to " + src.getAbsolutePath() + " This might take a while.");
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
