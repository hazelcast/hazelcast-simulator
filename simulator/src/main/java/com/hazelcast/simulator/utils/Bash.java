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
package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.coordinator.registry.IpAndPort;

import java.io.File;

import static java.lang.String.format;

public class Bash {

    private final String user;
    private final String sshOptions;
    private final String scpOptions;

    public Bash(SimulatorProperties simulatorProperties) {
        this.user = simulatorProperties.getUser();
        this.sshOptions = simulatorProperties.getSshOptions();
        this.scpOptions = sshOptions.replace("-t ", "").replace("-tt ", "");
    }

    public String execute(String command) {
        return new BashCommand(command).execute();
    }

    public String sshTTY(IpAndPort ipAndPort, String command) {
        return ssh(ipAndPort, command, false, true);
    }

    public String ssh(IpAndPort ipAndPort, String command) {
        return ssh(ipAndPort, command, false, false);
    }

    public String ssh(IpAndPort ipAndPort, String command, boolean throwException, boolean forceTTy) {
        String options = sshOptions + (forceTTy ? " -tt" : "");
        String sshCommand = format("ssh -p %s %s %s@%s \"%s\"", ipAndPort.getPort(), options, user, ipAndPort.getIp(), command);
        return new BashCommand(sshCommand).setThrowsException(throwException).execute();
    }

    public void sshQuiet(IpAndPort ipAndPort, String command) {
        ssh(ipAndPort, command + " || true");
    }

    public void killAllJavaProcesses(IpAndPort ipAndPort, boolean sudo) {
        if (sudo) {
            sshQuiet(ipAndPort, "sudo killall -9 java");
        } else {
            sshQuiet(ipAndPort, "killall -9 java");
        }
    }

    public void scpToRemote(IpAndPort ipAndPort, File src, String target) {
        String command = format("scp -P %s -r %s %s %s@%s:%s", ipAndPort.getPort(), scpOptions, src.getAbsolutePath(), user,
                ipAndPort.getIp(), target);
        execute(command);
    }
}
