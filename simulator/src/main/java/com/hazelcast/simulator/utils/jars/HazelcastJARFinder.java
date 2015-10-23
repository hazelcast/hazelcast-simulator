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

import com.hazelcast.simulator.utils.CommandLineExitException;

import java.io.File;

import static com.hazelcast.simulator.utils.FileUtils.newFile;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;

class HazelcastJARFinder {

    private static final String[] JAR_NAMES = new String[]{"hazelcast", "hazelcast-client", "hazelcast-wm"};

    public File[] find(File path) {
        return find(path, JAR_NAMES);
    }

    public File[] find(File path, String[] jarNames) {
        File[] jars = new File[jarNames.length];
        int index = 0;
        for (String jarName : jarNames) {
            File pathToTarget = newFile(path, jarName, "target");
            checkPathExist(pathToTarget);

            jars[index++] = findHazelcastJar(pathToTarget);
        }
        return jars;
    }

    private void checkPathExist(File pathToTarget) {
        if (!pathToTarget.exists()) {
            throw new CommandLineExitException("Cannot find a path to Hazelcast JAR. It should be at "
                    + pathToTarget.getAbsolutePath() + ", but the path doesn't exist.");
        }
    }

    private File findHazelcastJar(File pathToTarget) {
        File[] files = pathToTarget.listFiles(new HazelcastFilenameFilter());
        if (files == null) {
            throw new CommandLineExitException("Hazelcast JARs not found!");
        }
        checkIsSingleFile(files, pathToTarget);
        return files[0];
    }

    private void checkIsSingleFile(File[] files, File pathToTarget) {
        if (files.length == 0) {
            throw new CommandLineExitException("Path " + pathToTarget + " doesn't contain Hazelcast JAR.");
        }
        if (files.length > 1) {
            StringBuilder sb = new StringBuilder("Path ")
                    .append(pathToTarget.getAbsolutePath())
                    .append("contains more than one JAR which seems to be Hazelcast JAR: ").append(NEW_LINE);
            for (File file : files) {
                sb.append(file.getName()).append(NEW_LINE);
            }
            sb.append("This is probably a bug, please create a new bug report with this message."
                    + " https://github.com/hazelcast/hazelcast-simulator/issues/new");
            throw new CommandLineExitException(sb.toString());
        }
    }
}
