package com.hazelcast.stabilizer.provisioner;

import java.io.IOException;
import java.io.InputStream;

public class StreamGobbler extends Thread {
    private InputStream is;
    private StringBuffer sb;

    public StreamGobbler(InputStream is, StringBuffer sb) {
        this.is = is;
        this.sb = sb;
    }

    @Override
    public void run() {
        try {
            int c;
            while ((c = is.read()) != -1) {
                sb.append((char) c);
            }
        } catch (IOException x) {
            // handle error
        }
    }
}
