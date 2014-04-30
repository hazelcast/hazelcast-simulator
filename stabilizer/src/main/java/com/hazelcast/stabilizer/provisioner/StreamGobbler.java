package com.hazelcast.stabilizer.provisioner;

import java.io.IOException;
import java.io.InputStream;

class StreamGobbler extends Thread {
    InputStream is;
    StringBuffer sb;

    StreamGobbler(InputStream is, StringBuffer sb) {
        this.is = is;
        this.sb = sb;
    }

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
