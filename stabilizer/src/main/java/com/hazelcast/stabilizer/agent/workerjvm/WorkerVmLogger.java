/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.stabilizer.agent.workerjvm;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.logging.Level;

public class WorkerVmLogger extends Thread {
    private final static ILogger log = Logger.getLogger(WorkerVmLogger.class.getName());

    private final InputStream inputStream;
    private final String prefix;
    private final boolean workerTrackLogging;

    public WorkerVmLogger(String prefix, InputStream inputStream, boolean workerTrackLogging) {
        this.inputStream = inputStream;
        this.prefix = prefix;
        this.workerTrackLogging = workerTrackLogging;
    }

    public void run() {
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            for (; ; ) {
                final String line = br.readLine();
                if (line == null) {
                    break;
                }
                if (log.isLoggable(Level.INFO) && workerTrackLogging) {
                    log.info(prefix + ": " + line);
                }
            }
        } catch (IOException e) {
        }
    }
}
