package com.hazelcast.simulator.provisioner;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.utils.NativeUtils;

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

    public void uploadToAgentSimulatorDir(String ip, String src, String target) {
        String command = format("rsync -avv -e \"ssh %s\" %s %s@%s:hazelcast-simulator-%s/%s", sshOptions, src, user, ip,
                getSimulatorVersion(), target);
        execute(command);
    }

    public void scpToRemote(String ip, File src, String target) {
        String command = format("scp -r %s %s %s@%s:%s", sshOptions, src.getAbsolutePath(), user, ip, target);
        execute(command);
    }
}
