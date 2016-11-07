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

package com.hazelcast.simulator.boot;


import com.hazelcast.simulator.utils.FileUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static java.lang.String.format;
import static org.apache.commons.lang3.SystemUtils.getUserHome;

class SimulatorInstaller {

    private final String version = getSimulatorVersion();

    void install() {
        File simulatorHome = new File(FileUtils.getUserHome(), "hazelcast-simulator-" + version);
        System.setProperty("SIMULATOR_HOME", simulatorHome.getAbsolutePath());

        if (simulatorHome.exists()) {
            return;
        }

        System.out.println("Installing Simulator: " + version);

        try {
            URL url = getUrl();
            File archive = getTargetZipFile();
            if (archive.exists()) {
                System.out.printf("File [%s] already exist, skipping download\n", archive.getAbsolutePath());
            } else {
                ReadableByteChannel rbc = Channels.newChannel(url.openStream());
                System.out.printf("File [%s] doesn't exist; downloading\n", archive.getAbsolutePath());
                FileOutputStream fos = new FileOutputStream(archive);
                fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
            }

            ProcessBuilder pb = new ProcessBuilder(
                    "tar", "-xpf", archive.getAbsolutePath(), "-C", getUserHome().getAbsolutePath());
            pb.start().waitFor();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private File getTargetZipFile() {
        return new File(getUserHome(), format("hazelcast-simulator-%s-dist.tar.gz", version));
    }

    private URL getUrl() throws MalformedURLException {
        if (version.endsWith("SNAPSHOT")) {
            return new URL(
                    format("https://oss.sonatype.org/content/repositories/snapshots/maven2/"
                            + "com/hazelcast/simulator/dist/%s/dist-%s-dist.tar.gz", version, version));
        } else {
            return new URL(
                    format("http://repo1.maven.org/maven2/"
                            + "com/hazelcast/simulator/dist/%s/dist-%s-dist.tar.gz", version, version));
        }
    }

    public static void main(String[] args) {
        SimulatorInstaller installer = new SimulatorInstaller();
        installer.install();
    }
}
