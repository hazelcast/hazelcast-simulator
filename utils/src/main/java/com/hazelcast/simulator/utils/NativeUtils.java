/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.utils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;

import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.rethrow;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;

public final class NativeUtils {

    private static final Logger LOGGER = Logger.getLogger(NativeUtils.class);

    private NativeUtils() {
    }

    public static void execute(String command) {
        StringBuilder sb = new StringBuilder();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Executing bash command: " + command);
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
                LOGGER.error(String.format("Failed to execute [%s]", command));
                LOGGER.error(sb.toString());
                exitWithError();
            } else {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Bash output:" + NEW_LINE + sb);
                }
            }
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    public static void kill(int pid) {
        LOGGER.info("Sending -9 signal to PID " + pid);
        try {
            Runtime.getRuntime().exec("/bin/kill -9 " + pid + " >/dev/null");
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * http://stackoverflow.com/questions/35842/how-can-a-java-program-get-its-own-process-id
     *
     * @return the PID of this JVM or <tt>-1</tt> if not PID was retrieved
     */
    public static int getPID() {
        Integer pid = getPidFromManagementBean();
        if (pid != null) {
            return pid;
        }
        pid = getPidViaReflection();
        if (pid != null) {
            return pid;
        }
        return -1;
    }

    static Integer getPidFromManagementBean() {
        String name = ManagementFactory.getRuntimeMXBean().getName();
        return getPidFromBeanString(name);
    }

    static Integer getPidFromBeanString(String name) {
        int indexOf = name.indexOf('@');
        if (indexOf == -1) {
            return null;
        }
        String pidString = name.substring(0, indexOf);
        try {
            return Integer.parseInt(pidString);
        } catch (NumberFormatException e) {
            LOGGER.warn(e);
            return null;
        }
    }

    static Integer getPidViaReflection() {
        try {
            java.lang.management.RuntimeMXBean runtime = java.lang.management.ManagementFactory.getRuntimeMXBean();
            java.lang.reflect.Field jvm = runtime.getClass().getDeclaredField("jvm");
            jvm.setAccessible(true);
            sun.management.VMManagement management = (sun.management.VMManagement) jvm.get(runtime);
            java.lang.reflect.Method pidMethod = management.getClass().getDeclaredMethod("getProcessId");
            pidMethod.setAccessible(true);

            return (Integer) pidMethod.invoke(management);
        } catch (Exception e) {
            LOGGER.warn(e);
            return null;
        }
    }

    private static class BashStreamGobbler extends Thread {

        private final InputStreamReader inputStreamReader;
        private final BufferedReader reader;
        private final StringBuilder stringBuilder;

        @SuppressFBWarnings({"DM_DEFAULT_ENCODING"})
        public BashStreamGobbler(InputStream in, StringBuilder stringBuilder) {
            this.inputStreamReader = new InputStreamReader(in);
            this.reader = new BufferedReader(inputStreamReader);
            this.stringBuilder = stringBuilder;
        }

        @Override
        public void run() {
            try {
                String line;
                while ((line = reader.readLine()) != null) {
                    stringBuilder.append(line).append(NEW_LINE);
                }
            } catch (IOException ignored) {
                EmptyStatement.ignore(ignored);
            } finally {
                closeQuietly(reader);
                closeQuietly(inputStreamReader);
            }
        }
    }
}
