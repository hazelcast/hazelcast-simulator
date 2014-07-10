package com.hazelcast.stabilizer.provisioner;


import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.NativeUtils;
import com.hazelcast.stabilizer.common.StabilizerProperties;

import java.io.File;

import static com.hazelcast.stabilizer.Utils.getVersion;
import static java.lang.String.format;

public class Bash {
    private final static ILogger log = Logger.getLogger(Bash.class);

    private final String sshOptions;
    private final String user;

    public Bash(StabilizerProperties stabilizerProperties) {
        this.sshOptions = stabilizerProperties.get("SSH_OPTIONS");
        this.user = stabilizerProperties.get("USER");
    }

    public void execute(String command) {
        NativeUtils.execute(command);
    }

    public void copyToAgentStabilizerDir(String ip, String src, String target) {
        String syncCommand = format("rsync -av -e \"ssh %s\" %s %s@%s:hazelcast-stabilizer-%s/%s",
                sshOptions, src, user, ip, getVersion(), target);

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
