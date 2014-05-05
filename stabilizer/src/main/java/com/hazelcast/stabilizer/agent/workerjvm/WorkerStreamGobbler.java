package com.hazelcast.stabilizer.agent.workerjvm;

import com.hazelcast.stabilizer.Utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;

public class WorkerStreamGobbler extends Thread {

    private final PrintWriter writer;
    private final BufferedReader reader;

    public WorkerStreamGobbler(InputStream in, OutputStream out) {
        writer = new PrintWriter(out);
        reader = new BufferedReader(new InputStreamReader(in));
    }

    /**
     * @see Thread#run()
     */
    public void run() {
        try {

            String line;
            while ((line = reader.readLine()) != null) {
                writer.append(line + "\n");
            }
        } catch (IOException ioException) {
            //LOGGER.warn("System command stream gobbler error", ioException);
        } finally {
            Utils.closeQuietly(writer);
            Utils.closeQuietly(reader);
        }
    }
}