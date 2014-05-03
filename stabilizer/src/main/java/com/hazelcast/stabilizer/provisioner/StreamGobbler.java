package com.hazelcast.stabilizer.provisioner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

class StreamGobbler extends Thread {

    private final InputStream in;

    private StringBuffer response;

    StreamGobbler(InputStream in, StringBuffer response) {
        this.in = in;
        this.response = response;
    }

    /**
     * @see java.lang.Thread#run()
     */
    public void run() {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line;
            while ((line = reader.readLine()) != null) {
                response.append(line + "\n");
            }
        } catch (IOException ioException) {
            //LOGGER.warn("System command stream gobbler error", ioException);
        }
    }

}