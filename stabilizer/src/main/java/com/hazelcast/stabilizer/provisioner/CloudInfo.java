package com.hazelcast.stabilizer.provisioner;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.common.StabilizerProperties;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.domain.Hardware;
import org.jclouds.compute.domain.Image;
import org.jclouds.domain.Location;

import java.util.Set;

import static com.hazelcast.stabilizer.Utils.getStablizerHome;
import static com.hazelcast.stabilizer.Utils.getVersion;
import static java.lang.String.format;

public class CloudInfo {
    private final static ILogger log = Logger.getLogger(CloudInfo.class);

    public StabilizerProperties props = new StabilizerProperties();

    private ComputeService computeService;
    public String locationId;
    public boolean verbose;

    public void init() {
        computeService = new ComputeServiceBuilder(props).build();
    }

    //show all support clouds

    public void showLocations() {
        Set<? extends Location> locations = computeService.listAssignableLocations();
        for (Location location : locations) {
            System.out.println(location);
        }
    }

    public void showHardware() {
        Set<? extends Hardware> hardwares = computeService.listHardwareProfiles();
        for (Hardware hardware : hardwares) {
            if(verbose) {
                System.out.println(hardware);
            }else{
                StringBuilder sb = new StringBuilder(hardware.getId());
                sb.append(" Ram: ").append(hardware.getRam());
                sb.append(" Processors: ").append(hardware.getProcessors());
                System.out.println(sb.toString());
            }
        }
    }

    public void showImages() {
        Set<? extends Image> images = computeService.listImages();
        for (Image image : images) {
            boolean match = locationId == null || image.getLocation().getId().equals(locationId);
            if(!match){
                continue;
            }

            if(verbose) {
                System.out.println(image);
            }else{
                StringBuilder sb = new StringBuilder(image.getId());
                sb.append(" OS: ").append(image.getOperatingSystem());
            //    sb.append(" Version: ").append(image.getVersion());
                System.out.println(sb.toString());
            }
        }
    }

    public static void main(String[] args) {
        log.info("Hazelcast Stabilizer CloudInfo");
        log.info(format("Version: %s", getVersion()));
        log.info(format("STABILIZER_HOME: %s", getStablizerHome()));

        try {
            CloudInfo cloudInfoCli = new CloudInfo();
            CloudInfoCli cli = new CloudInfoCli(cloudInfoCli);
            cli.run(args);
            System.exit(0);
        } catch (Throwable e) {
            log.severe(e);
            System.exit(1);
        }
    }
}
