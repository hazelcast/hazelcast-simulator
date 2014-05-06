package com.hazelcast.stabilizer.provisioner;


import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.IOException;
import java.util.Properties;

import static java.lang.String.format;

public class Shell {
    private final static ILogger log = Logger.getLogger(Shell.class);
    private final Properties properties;

    Shell(Properties properties) {
        this.properties = properties;
    }

    void bash(String command) {
        StringBuffer sout = new StringBuffer();

        log.finest("Executing bash command: " + command);

        try {
            // create a process for the shell
            ProcessBuilder pb = new ProcessBuilder("bash", "-c", command);
            pb = pb.redirectErrorStream(true);

            Process shell = pb.start();
            new StringBufferStreamGobbler(shell.getInputStream(), sout).start();

            // wait for the shell to finish and get the return code
            int shellExitStatus = shell.waitFor();

            if (shellExitStatus != 0) {
                log.info(String.format("Failed to execute [%s]", command));
                log.severe(sout.toString());
                System.exit(1);
            } else {
                log.finest("Bash output: \n" + sout);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    void scpToRemote(String ip, String src, String target) {
        String command = format("scp -r %s %s %s@%s:%s",
                properties.get("SSH_OPTIONS"), src, properties.get("USER"), ip, target);
        bash(command);
    }

    void ssh(String ip, String command) {
        String sshCommand = format("ssh %s -q %s@%s \"%s\"",
                properties.get("SSH_OPTIONS"), properties.get("USER"), ip, command);
        bash(sshCommand);
    }

    void sshQuiet(String ip, String command) {
        String sshCommand = format("ssh %s -q %s@%s \"%s\" || true",
                properties.get("SSH_OPTIONS"), properties.get("USER"), ip, command);
        bash(sshCommand);
    }

}
