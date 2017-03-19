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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;

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
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static com.hazelcast.simulator.utils.Preconditions.checkNotNull;
import static java.lang.String.format;

public class BashCommand {
    private static final Logger LOGGER = Logger.getLogger(BashCommand.class);
    private static final String INFO = "[INFO]";
    private static final String WARN = "[WARN]";
    private static final String ERROR = "[ERROR]";

    private final List<String> params = new ArrayList<String>();
    private final Map<String, Object> environment = new HashMap<String, Object>();
    private boolean throwException;
    private File directory;
    private boolean dumpOutputOnError = true;

    public BashCommand(String command) {
        params.add(command);
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

    public BashCommand setDirectory(File directory) {
        this.directory = checkNotNull(directory, "directory can't be null");
        return this;
    }

    public BashCommand dumpOutputOnError(boolean dumpOutputOnError) {
        this.dumpOutputOnError = dumpOutputOnError;
        return this;
    }

    private String command() {
        StringBuilder sb = new StringBuilder();
        for (String param : params) {
            sb.append(param).append(' ');
        }
        return sb.toString();
    }

    public String execute() {
        StringBuilder sb = new StringBuilder();

        String command = command();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Executing bash command: " + command);
        }

        try {
            // create a process for the shell
            ProcessBuilder pb = new ProcessBuilder("bash", "-c", command);
            if (directory != null) {
                pb.directory(directory);
            }

            // fix the environment
            for (Map.Entry<String, Object> entry : environment.entrySet()) {
                pb.environment().put(entry.getKey(), entry.getValue().toString());
            }
            pb = pb.redirectErrorStream(true);

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

    private static class BashStreamGobbler extends Thread {
        private final InputStreamReader inputStreamReader;
        private final BufferedReader reader;
        private final StringBuilder stringBuilder;

        @SuppressFBWarnings("DM_DEFAULT_ENCODING")
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
                    if (line.startsWith(ERROR)) {
                        LOGGER.error(line.substring(ERROR.length(), line.length()));
                    } else if (line.startsWith(WARN)) {
                        LOGGER.warn(line.substring(WARN.length(), line.length()));
                    } else if (line.startsWith(INFO)) {
                        LOGGER.info(line.substring(INFO.length(), line.length()));
                    } else {
                        LOGGER.trace(line);
                    }
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
