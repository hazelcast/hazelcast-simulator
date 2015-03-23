package com.hazelcast.simulator.provisioner;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.utils.NativeUtils;

import java.io.File;

import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
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

    /**
     * Downloads the content of the url to the target path.
     *
     * @param path the directory where the content is stored
     * @param url  the url that is downloaded
     */
    public void download(String path, String url) {
        execute("if type \"wget\" > /dev/null; \n"
                + "then \n"
                + "\twget --no-verbose --directory-prefix=" + path + " " + url + " \n"
                + "else\n"
                + "\t\t\n"
                + "\tpushd .\n"
                + "\tcd " + path + "\t\n"
                + "\tcurl -O " + url + "\n"
                + "\tpopd\n"
                + "fi");
    }

    public void uploadToAgentSimulatorDir(String ip, String src, String target) {
        String syncCommand = format("rsync -avv -e \"ssh %s\" %s %s@%s:hazelcast-simulator-%s/%s",
                sshOptions, src, user, ip, getSimulatorVersion(), target);

        execute(syncCommand);
    }

    public void scpToRemote(String ip, File src, String target) {
        scpToRemote(ip, src.getAbsolutePath(), target);
    }

    public void scpToRemote(String ip, String src, String target) {
        String command = format("scp -r %s %s %s@%s:%s", sshOptions, src, user, ip, target);
        execute(command);
    }

    public void ssh(String ip, String command) {
        String sshCommand = format("ssh %s %s@%s \"%s\"", sshOptions, user, ip, command);
        execute(sshCommand);
    }

    public void sshQuiet(String ip, String command) {
        String sshCommand = format("ssh %s %s@%s \"%s\" || true", sshOptions, user, ip, command);
        execute(sshCommand);
    }
}
