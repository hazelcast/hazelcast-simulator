package com.hazelcast.simulator.provisioner;

import com.hazelcast.simulator.common.SimulatorProperties;
import org.apache.log4j.Logger;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.domain.Hardware;
import org.jclouds.compute.domain.Image;
import org.jclouds.domain.Location;

import java.util.Set;

import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static java.lang.String.format;

/**
 * Commandline tool to retrieve various cloud info.
 */
public class CloudInfo {

    private static final Logger LOGGER = Logger.getLogger(CloudInfo.class);

    public SimulatorProperties props = new SimulatorProperties();

    public String locationId;
    public boolean verbose;
    private ComputeService computeService;

    public void init() {
        computeService = new ComputeServiceBuilder(props).build();
    }

    // show all support clouds
    public void showLocations() {
        Set<? extends Location> locations = computeService.listAssignableLocations();
        for (Location location : locations) {
            System.out.println(location);
        }
    }

    public void showHardware() {
        Set<? extends Hardware> hardwareSet = computeService.listHardwareProfiles();
        for (Hardware hardware : hardwareSet) {
            if (verbose) {
                System.out.println(hardware);
            } else {
                StringBuilder sb = new StringBuilder(hardware.getId());
                sb.append(" Ram: ").append(hardware.getRam());
                sb.append(" Processors: ").append(hardware.getProcessors());
                if (locationId == null) {
                    Location location = hardware.getLocation();
                    if (location != null) {
                        sb.append(" Location: ").append(location.getId());
                    }
                }
                System.out.println(sb.toString());
            }
        }
    }

    public void showImages() {
        Set<? extends Image> images = computeService.listImages();
        for (Image image : images) {
            boolean match = show(image);
            if (!match) {
                continue;
            }

            if (verbose) {
                System.out.println(image);
            } else {
                System.out.println(image.getId() + " OS: " + image.getOperatingSystem() + " Version: " + image.getVersion());
            }
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
            CloudInfoCli cli = new CloudInfoCli(cloudInfoCli);
            cli.run(args);
            System.exit(0);
        } catch (Throwable e) {
            LOGGER.fatal(e);
            System.exit(1);
        }
    }
}
