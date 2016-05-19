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
package com.hazelcast.simulator.remotecontroller;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.protocol.connector.RemoteControllerConnector;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.operation.RemoteControllerOperation;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.apache.log4j.Logger;

import static com.hazelcast.simulator.common.GitInfo.getBuildTime;
import static com.hazelcast.simulator.common.GitInfo.getCommitIdAbbrev;
import static com.hazelcast.simulator.protocol.operation.RemoteControllerOperation.Type;
import static com.hazelcast.simulator.remotecontroller.RemoteControllerCli.init;
import static com.hazelcast.simulator.remotecontroller.RemoteControllerCli.run;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static java.lang.String.format;

/**
 * Commandline tool to send {@link RemoteControllerOperation} to the Coordinator.
 */
@SuppressWarnings("checkstyle:hideutilityclassconstructor")
public class RemoteController {

    private static final Logger LOGGER = Logger.getLogger(RemoteController.class);

    private RemoteControllerConnector connector;
    private boolean isQuiet;

    RemoteController(SimulatorProperties simulatorProperties, boolean isQuiet) {
        int coordinatorPort = simulatorProperties.getCoordinatorPort();
        if (coordinatorPort == 0) {
            throw new CommandLineExitException("Coordinator port is disabled!");
        }

        this.connector = new RemoteControllerConnector("localhost", coordinatorPort);
        this.isQuiet = isQuiet;
    }

    // just for testing
    void setRemoteControllerConnector(RemoteControllerConnector connector) {
        this.connector = connector;
    }

    void start() {
        connector.start();
    }

    void shutdown() {
        connector.shutdown();
    }

    void listComponents() {
        sendOperation(Type.LIST_COMPONENTS);
        log(connector.getResponse());
    }

    void sendOperation(Type operationType) {
        Response response = connector.write(new RemoteControllerOperation(operationType));
        ResponseType responseType = response.getFirstErrorResponseType();
        if (responseType != ResponseType.SUCCESS) {
            throw new CommandLineExitException("Could not process command: " + responseType);
        }
    }

    void log(String message) {
        if (isQuiet) {
            System.out.println(message);
        } else {
            echo(message);
        }
    }

    public static void main(String[] args) {
        try {
            run(args, init(args));
        } catch (Exception e) {
            exitWithError(LOGGER, "Error during execution of Remote Controller!", e);
        }
    }

    static void logHeader() {
        echo("Hazelcast Simulator Remote Controller");
        echo("Version: %s, Commit: %s, Build Time: %s", getSimulatorVersion(), getCommitIdAbbrev(), getBuildTime());
        echo("SIMULATOR_HOME: %s", getSimulatorHome().getAbsolutePath());
    }

    private static void echo(String message, Object... args) {
        LOGGER.info(message == null ? "null" : format(message, args));
    }
}
