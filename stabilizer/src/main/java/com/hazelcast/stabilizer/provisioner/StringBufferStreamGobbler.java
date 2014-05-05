package com.hazelcast.stabilizer.provisioner;

import com.hazelcast.stabilizer.Utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class StringBufferStreamGobbler extends Thread {
    private final BufferedReader reader;
    private final StringBuffer stringBuffer;

    public StringBufferStreamGobbler(InputStream in, StringBuffer stringBuffer) {
        reader = new BufferedReader(new InputStreamReader(in));
        this.stringBuffer = stringBuffer;
    }

    @Override
    public void run() {
        try {
            String line;
            while ((line = reader.readLine()) != null) {
                stringBuffer.append(line + "\n");
            }
        } catch (IOException ioException) {
            //LOGGER.warn("System command stream gobbler error", ioException);
        } finally {
            Utils.closeQuietly(reader);
        }
    }
}