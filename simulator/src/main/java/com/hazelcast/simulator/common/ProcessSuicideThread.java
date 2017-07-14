/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.common;

import com.hazelcast.simulator.utils.BashCommand;
import org.apache.log4j.Logger;

import static com.hazelcast.simulator.utils.EmptyStatement.ignore;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Checks if the parent process is still running. If not, the current process is terminated.
 *
 * This helps to prevent 'orphan' workers which especially in a local setup are a problem since one can't just kill all
 * java processes since this would also kill the IDE.
 */
public final class ProcessSuicideThread extends Thread {
    private static final Logger LOGGER = Logger.getLogger(ProcessSuicideThread.class);

    private final String parentPid;
    private final int intervalSeconds;

    public ProcessSuicideThread(String parentPid, int intervalSeconds) {
        super("ProcessSuicideThread");
        setDaemon(true);
        this.parentPid = parentPid;
        this.intervalSeconds = intervalSeconds;
    }

    @Override
    public void run() {
        if (parentPid == null || intervalSeconds == 0) {
            return;
        }

        try {
            for (; ; ) {
                SECONDS.sleep(intervalSeconds);
                BashCommand bashCommand = new BashCommand("ps -p " + parentPid);
                bashCommand.setThrowsException(true);

                try {
                    bashCommand.execute();
                } catch (Exception e) {
                    String msg = "Worker terminating; agent with pid [" + parentPid + "] is not alive";
                    LOGGER.error(msg);
                    System.err.println(msg);
                    System.exit(1);
                }
            }
        } catch (InterruptedException e) {
            ignore(e);
        }
    }
}
