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
package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.common.SimulatorProperties;

import java.io.File;

import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static java.lang.String.format;

public class Bash {

    private final String sshOptions;
    private final String user;

    public Bash(SimulatorProperties simulatorProperties) {
        this.sshOptions = simulatorProperties.get("SSH_OPTIONS");
        this.user = simulatorProperties.get("USER");
    }

    public void execute(String command) {
        NativeUtils.execute(command);
    }

    public void executeQuiet(String command) {
        execute(command + " || true");
    }

    public void ssh(String ip, String command) {
        String sshCommand = format("ssh %s %s@%s \"%s\"", sshOptions, user, ip, command);
        execute(sshCommand);
    }

    public void sshQuiet(String ip, String command) {
        ssh(ip, command + " || true");
    }

    public void killAllJavaProcesses(String ip) {
        sshQuiet(ip, "killall -9 java");
    }

    /**
     * Downloads the content of the url to the target path.
     *
     * @param url    the url that is downloaded
     * @param target the directory where the content will be stored
     */
    public void download(String url, String target) {
        execute("if type \"wget\" > /dev/null;" + NEW_LINE
                + "then" + NEW_LINE
                + "\twget --no-verbose --directory-prefix=" + target + ' ' + url + NEW_LINE
                + "else" + NEW_LINE
                + "\tpushd ." + NEW_LINE
                + "\tcd " + target + NEW_LINE
                + "\tcurl -O " + url + NEW_LINE
                + "\tpopd" + NEW_LINE
                + "fi");
    }

    public void uploadToRemoteSimulatorDir(String ip, String src, String target) {
        String command = format("rsync --checksum -avv -e \"ssh %s\" %s %s@%s:hazelcast-simulator-%s/%s",
                sshOptions, src, user, ip, getSimulatorVersion(), target);
        execute(command);
    }

    public void scpToRemote(String ip, File src, String target) {
        String command = format("scp -r %s %s %s@%s:%s", sshOptions, src.getAbsolutePath(), user, ip, target);
        execute(command);
    }
}
