package com.hazelcast.stabilizer.provisioner.git;

import com.hazelcast.stabilizer.provisioner.Bash;
import org.apache.log4j.Logger;

import java.io.File;

import static com.hazelcast.stabilizer.utils.CommonUtils.exitWithError;
import static com.hazelcast.stabilizer.utils.FileUtils.newFile;
import static java.lang.String.format;

public class BuildSupport {
    private final static Logger log = Logger.getLogger(BuildSupport.class);

    private final Bash bash;
    private final HazelcastJARFinder jarFinder;
    private final String mavenExecutable;

    public BuildSupport(Bash bash, HazelcastJARFinder jarFinder) {
        this(bash, jarFinder, null);
    }

    public BuildSupport(Bash bash, HazelcastJARFinder jarFinder, String pathToMaven) {
        this.bash = bash;
        this.jarFinder = jarFinder;
        this.mavenExecutable = getMavenExecutable(pathToMaven);
    }

    private String getMavenExecutable(String pathToMaven) {
        if (pathToMaven == null || pathToMaven.isEmpty()) {
            return "mvn";
        } else {
            File maven = new File(pathToMaven);
            if (!maven.exists()) {
                exitWithError(log, "Specified path " + pathToMaven + " to Maven doesn't exist.");
            }
            if (maven.isDirectory()) {
                maven = newFile(pathToMaven, "mvn");
            }
            checkIsValidMavenExecutable(pathToMaven, maven);
            return maven.getAbsolutePath();
        }
    }

    private void checkIsValidMavenExecutable(String pathToMaven, File mavenExec) {
        if (!mavenExec.isFile()) {
            exitWithError(log, "Specified path " + pathToMaven + " to Maven doesn't contains 'mvn' executable.");
        }
        if (!mavenExec.canExecute()) {
            exitWithError(log, "Specified path " + pathToMaven + " to Maven contains 'mvn' which is not executable.");
        }
    }

    public File[] build(File pathToSource) {
        String absolutePath = pathToSource.getAbsolutePath();
        log.info("Building Hazelcast from sources at " + absolutePath);

        String cmd = getBuildCommand(absolutePath);
        bash.execute(cmd);

        File[] jars = jarFinder.find(pathToSource);
        logFilenames(jars);
        return jars;
    }

    private String getBuildCommand(String absolutePath) {
        //TODO: This is probably not very safe, some escaping is probably necessary
        return format("cd %s ; %s clean install -DskipTests", absolutePath, mavenExecutable);
    }

    private void logFilenames(File[] jars) {
        StringBuilder sb = new StringBuilder("Hazelcast has been built successfully. JARs found: \n");
        for (File jar : jars) {
            sb.append(jar.getName()).append('\n');
        }
    }

}
