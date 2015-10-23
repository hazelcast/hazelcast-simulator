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
package com.hazelcast.simulator.provisioner;

import com.hazelcast.simulator.common.SimulatorProperties;
import org.apache.log4j.Logger;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.domain.Hardware;
import org.jclouds.compute.domain.Image;
import org.jclouds.domain.Location;

import java.util.Set;

import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static java.lang.String.format;

/**
 * Commandline tool to retrieve various cloud info.
 */
public final class CloudInfo {

    private static final Logger LOGGER = Logger.getLogger(CloudInfo.class);

    final SimulatorProperties props = new SimulatorProperties();

    String locationId;
    boolean verbose;

    private ComputeService computeService;

    void init() {
        computeService = new ComputeServiceBuilder(props).build();
    }

    void shutdown() {
        if (computeService != null) {
            computeService.getContext().close();
        }
    }

    // show all support clouds
    void showLocations() {
        Set<? extends Location> locations = computeService.listAssignableLocations();
        for (Location location : locations) {
            LOGGER.info(location);
        }
    }

    void showHardware() {
        Set<? extends Hardware> hardwareSet = computeService.listHardwareProfiles();
        for (Hardware hardware : hardwareSet) {
            if (verbose) {
                LOGGER.info(hardware);
                continue;
            }
            StringBuilder sb = new StringBuilder(hardware.getId());
            sb.append(" Ram: ").append(hardware.getRam());
            sb.append(" Processors: ").append(hardware.getProcessors());
            if (locationId == null) {
                Location location = hardware.getLocation();
                if (location != null) {
                    sb.append(" Location: ").append(location.getId());
                }
            }
            LOGGER.info(sb.toString());
        }
    }

    void showImages() {
        Set<? extends Image> images = computeService.listImages();
        for (Image image : images) {
            boolean match = show(image);
            if (!match) {
                continue;
            }
            if (verbose) {
                LOGGER.info(image);
                continue;
            }
            LOGGER.info(image.getId() + " OS: " + image.getOperatingSystem() + " Version: " + image.getVersion());
        }
    }

    private boolean show(Image image) {
        if (locationId == null) {
            return true;
        }

        Location imageLocation = image.getLocation();
        if (imageLocation == null) {
            return true;
        }

        String imageLocationId = imageLocation.getId();
        if (imageLocationId == null) {
            return true;
        }

        return locationId.equals(imageLocationId);
    }

    public static void main(String[] args) {
        LOGGER.info("Hazelcast Simulator CloudInfo");
        LOGGER.info(format("Version: %s", getSimulatorVersion()));
        LOGGER.info(format("SIMULATOR_HOME: %s", getSimulatorHome()));

        try {
            CloudInfo cloudInfoCli = new CloudInfo();
            CloudInfoCli cli = new CloudInfoCli(cloudInfoCli, args);

            cli.run();
        } catch (Exception e) {
            exitWithError(LOGGER, "Could not retrieve cloud information!", e);
        }
    }
}
