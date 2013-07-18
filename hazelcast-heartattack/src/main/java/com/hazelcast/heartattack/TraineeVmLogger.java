package com.hazelcast.heartattack;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.logging.Level;

public class TraineeVmLogger extends Thread {
    private final static ILogger log = Logger.getLogger(TraineeVmLogger.class.getName());

    private final InputStream inputStream;
    private final String prefix;
    private final boolean traineeTrackLogging;

    public TraineeVmLogger(String prefix, InputStream inputStream, boolean traineeTrackLogging) {
        this.inputStream = inputStream;
        this.prefix = prefix;
        this.traineeTrackLogging = traineeTrackLogging;
    }

    public void run() {
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            for (; ; ) {
                final String line = br.readLine();
                if (line == null) break;
                if (log.isLoggable(Level.INFO) && traineeTrackLogging) {
                    log.log(Level.INFO, prefix + ": " + line);
                }
            }
        } catch (IOException e) {
        }
    }
}
