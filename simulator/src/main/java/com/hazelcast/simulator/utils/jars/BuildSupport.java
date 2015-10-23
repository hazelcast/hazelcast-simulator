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

import com.hazelcast.simulator.utils.Bash;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.apache.log4j.Logger;

import java.io.File;

import static com.hazelcast.simulator.utils.FileUtils.newFile;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static java.lang.String.format;

class BuildSupport {

    private static final Logger LOGGER = Logger.getLogger(BuildSupport.class);

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
                throw new CommandLineExitException("Specified path " + pathToMaven + " to Maven doesn't exist.");
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
            throw new CommandLineExitException("Specified path " + pathToMaven + " to Maven doesn't contains 'mvn' executable.");
        }
        if (!mavenExec.canExecute()) {
            throw new CommandLineExitException("Specified path " + pathToMaven
                    + " to Maven contains 'mvn' which is not executable.");
        }
    }

    public File[] build(File pathToSource) {
        String absolutePath = pathToSource.getAbsolutePath();
        LOGGER.info("Building Hazelcast from sources at " + absolutePath);

        String cmd = getBuildCommand(absolutePath);
        bash.execute(cmd);

        File[] jars = jarFinder.find(pathToSource);
        logFilenames(jars);
        return jars;
    }

    private String getBuildCommand(String absolutePath) {
        // TODO: this is probably not very safe, some escaping is probably necessary
        return format("cd %s; %s clean install -DskipTests", absolutePath, mavenExecutable);
    }

    private void logFilenames(File[] jars) {
        StringBuilder sb = new StringBuilder("Hazelcast has been built successfully. JARs found:");
        sb.append(NEW_LINE);
        for (File jar : jars) {
            sb.append(jar.getName()).append(NEW_LINE);
        }
        LOGGER.info(sb.toString());
    }
}
