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
package com.hazelcast.simulator.provisioner;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.utils.CommandLineExitException;

import java.io.File;

import static com.hazelcast.simulator.utils.CloudProviderUtils.isCloudProvider;
import static com.hazelcast.simulator.utils.CloudProviderUtils.isLocal;
import static java.lang.String.format;

final class ProvisionerUtils {

    static final String INIT_SH_SCRIPT_NAME = "init.sh";

    private ProvisionerUtils() {
    }

    static File getInitScriptFile(String simulatorHome) {
        File initScript = new File(INIT_SH_SCRIPT_NAME);
        if (!initScript.exists()) {
            initScript = new File(simulatorHome + "/conf/" + INIT_SH_SCRIPT_NAME);
        }
        if (!initScript.exists()) {
            throw new CommandLineExitException(format("Could not find %s: %s", INIT_SH_SCRIPT_NAME, initScript));
        }
        return initScript;
    }

    static void ensureIsRemoteSetup(SimulatorProperties properties, String action) {
        if (isLocal(properties)) {
            throw new CommandLineExitException(format("Cannot execute '%s' in local setup", action));
        }
    }

    static void ensureIsCloudProviderSetup(SimulatorProperties properties, String action) {
        if (!isCloudProvider(properties)) {
            throw new CommandLineExitException(format("Cannot execute '%s' in local or static setup", action));
        }
    }
}
