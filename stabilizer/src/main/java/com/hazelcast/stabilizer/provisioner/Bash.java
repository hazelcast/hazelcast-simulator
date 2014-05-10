package com.hazelcast.stabilizer.provisioner;


import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.common.StabilizerProperties;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

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
        StringBuffer sb = new StringBuffer();

        if (log.isFinestEnabled()) {
            log.finest("Executing bash command: " + command);
        }

        try {
            // create a process for the shell
            ProcessBuilder pb = new ProcessBuilder("bash", "-c", command);
            pb = pb.redirectErrorStream(true);

            Process shell = pb.start();
            new BashStreamGobbler(shell.getInputStream(), sb).start();

            // wait for the shell to finish and get the return code
            int shellExitStatus = shell.waitFor();

            if (shellExitStatus != 0) {
                log.severe(String.format("Failed to execute [%s]", command));
                log.severe(sb.toString());
                System.exit(1);
            } else {
                if (log.isFinestEnabled()) {
                    log.finest("Bash output: \n" + sb);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
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

    public static class BashStreamGobbler extends Thread {
        private final BufferedReader reader;
        private final StringBuffer stringBuffer;

        public BashStreamGobbler(InputStream in, StringBuffer stringBuffer) {
            this.reader = new BufferedReader(new InputStreamReader(in));
            this.stringBuffer = stringBuffer;
        }

        @Override
        public void run() {
            try {
                String line;
                while ((line = reader.readLine()) != null) {
                    stringBuffer.append(line).append("\n");
                }
            } catch (IOException ioException) {
                //LOGGER.warn("System command stream gobbler error", ioException);
            } finally {
                Utils.closeQuietly(reader);
            }
        }
    }
}
