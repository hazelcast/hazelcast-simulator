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
package com.hazelcast.simulator.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.rethrow;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static com.hazelcast.simulator.utils.Preconditions.checkNotNull;
import static java.lang.String.format;

public class BashCommand {
    private static final Logger LOGGER = LogManager.getLogger(BashCommand.class);
    private static final String INFO = "[INFO]";
    private static final String WARN = "[WARN]";
    private static final String ERROR = "[ERROR]";

    private final List<String> params = new ArrayList<>();
    private final Map<String, Object> environment = new HashMap<>();
    private boolean throwException;
    private File directory;
    private boolean dumpOutputOnError = true;
    private boolean systemOut;
    private boolean ensureJavaOnPath;

    public BashCommand(String command) {
        params.add(command);

        environment.put("SIMULATOR_HOME", getSimulatorHome());
    }

    public BashCommand ensureJavaOnPath() {
        this.ensureJavaOnPath = true;
        return this;
    }

    public BashCommand addParams(Object... params) {
        for (Object param : params) {
            if (param == null) {
                this.params.add("null");
            } else if ("".equals(param)) {
                this.params.add("\"\"");
            } else {
                this.params.add('"' + param.toString() + '"');
            }
        }
        return this;
    }

    public BashCommand setSystemOut(boolean systemOut) {
        this.systemOut = systemOut;
        return this;
    }

    public BashCommand addEnvironment(Map<String, ? extends Object> environment) {
        this.environment.putAll(environment);
        return this;
    }

    public BashCommand addEnvironment(String variable, Object value) {
        this.environment.put(variable, value);
        return this;
    }

    public BashCommand setThrowsException(boolean throwException) {
        this.throwException = throwException;
        return this;
    }

    public BashCommand dumpOutputOnError(boolean dumpOutputOnError) {
        this.dumpOutputOnError = dumpOutputOnError;
        return this;
    }

    public BashCommand setDirectory(File directory) {
        this.directory = checkNotNull(directory, "directory can't be null");
        return this;
    }

    private String command() {
        StringBuilder sb = new StringBuilder();
        for (String param : params) {
            sb.append(param).append(' ');
        }
        String command =  sb.toString();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Executing bash command: " + command);
        }

        return command;
    }

    public String execute() {
        StringBuffer sb = new StringBuffer();

        String command = command();

        try {
            // create a process for the shell
            ProcessBuilder pb = newProcessBuilder(command);

            Process shell = pb.start();
            new BashStreamGobbler(shell.getInputStream(), sb).start();

            // wait for the shell to finish and get the return code
            int shellExitStatus = shell.waitFor();

            if (shellExitStatus != 0) {
                if (throwException) {
                    throw new ScriptException(format("Failed to execute [%s]", command));
                }
                if (dumpOutputOnError) {
                    LOGGER.error(format("Failed to execute [%s]", command));
                    LOGGER.error(sb.toString());
                }
                exitWithError();
            } else {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Bash output: " + NEW_LINE + sb);
                }
            }

            return sb.toString();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private ProcessBuilder newProcessBuilder(String command) {
        ProcessBuilder pb = new ProcessBuilder("bash", "-c", command);
        fixDirectory(pb);
        fixEnvironment(pb);
        fixPath(pb);
        pb.redirectErrorStream(true);
        return pb;
    }

    private void fixDirectory(ProcessBuilder pb) {
        if (directory != null) {
            pb.directory(directory);
        }
    }

    private void fixPath(ProcessBuilder pb) {
        if (ensureJavaOnPath) {
            String path = pb.environment().get("PATH");
            String newPath = path + ":" + System.getProperty("java.home") + "/bin";
            pb.environment().put("PATH", newPath);
        }
    }

    private void fixEnvironment(ProcessBuilder pb) {
        for (Map.Entry<String, Object> entry : environment.entrySet()) {
            pb.environment().put(entry.getKey(), entry.getValue().toString());
        }
    }

    private class BashStreamGobbler extends Thread {
        private final InputStreamReader inputStreamReader;
        private final BufferedReader reader;
        private final StringBuffer stringBuffer;

        public BashStreamGobbler(InputStream in, StringBuffer stringBuffer) {
            this.inputStreamReader = new InputStreamReader(in);
            this.reader = new BufferedReader(inputStreamReader);
            this.stringBuffer = stringBuffer;
        }

        @Override
        public void run() {
            try {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.startsWith(ERROR)) {
                        String s = line.substring(ERROR.length(), line.length());
                        if (systemOut) {
                            System.out.println(s);
                        }
                        LOGGER.error(s);
                    } else if (line.startsWith(WARN)) {
                        String s = line.substring(WARN.length(), line.length());
                        if (systemOut) {
                            System.out.println(s);
                        }
                        LOGGER.warn(s);
                    } else if (line.startsWith(INFO)) {
                        String s = line.substring(INFO.length(), line.length());
                        if (systemOut) {
                            System.out.println(s);
                        }
                        LOGGER.info(s);
                    } else {
                        if (systemOut) {
                            System.out.println(line);
                        }
                        LOGGER.trace(line);
                    }
                    if (!systemOut) {
                        stringBuffer.append(line).append(NEW_LINE);
                    }
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
