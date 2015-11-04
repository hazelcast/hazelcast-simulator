package com.hazelcast.simulator.provisioner;

import org.jclouds.compute.domain.Hardware;
import org.jclouds.compute.domain.Image;
import org.jclouds.domain.Location;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static java.util.Collections.singleton;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CloudInfoTest extends AbstractComputeServiceTest {

    private CloudInfo cloudInfo;

    @Before
    public void setUp() {
        initComputeServiceMock();

        cloudInfo = new CloudInfo(null, false, computeService);
    }

    @After
    public void tearDown() {
        cloudInfo.shutdown();
    }

    @Test
    public void testShowLocations() throws Exception {
        Set<? extends Location> locations = singleton(mock(Location.class));
        doReturn(locations).when(computeService).listAssignableLocations();

        cloudInfo.showLocations();

        verify(computeService).listAssignableLocations();
    }

    @Test
    public void testShowHardware() {
        mockComputeServiceForListHardware();

        cloudInfo.showHardware();

        verify(computeService).listHardwareProfiles();
    }

    @Test
    public void testShowHardware_verbose() {
        mockComputeServiceForListHardware();

        cloudInfo = new CloudInfo(null, true, computeService);
        cloudInfo.showHardware();

        verify(computeService).listHardwareProfiles();
    }

    @Test
    public void testShowImages() {
        Set<? extends Image> images = singleton(mock(Image.class));
        doReturn(images).when(computeService).listImages();

        cloudInfo.showImages();

        verify(computeService).listImages();
    }

    @Test
    public void testShowImages_withLocationId_verbose() {
        mockComputeServiceForListImages(null);

        cloudInfo = new CloudInfo("locationId", true, computeService);
        cloudInfo.showImages();

        verify(computeService).listImages();
    }

    @Test
    public void testShowImages_withLocationId_witImageLocation() {
        mockComputeServiceForListImages(mock(Location.class));

        cloudInfo = new CloudInfo("locationId", false, computeService);
        cloudInfo.showImages();

        verify(computeService).listImages();
    }

    @Test
    public void testShowImages_withLocationId_witImageLocationId() {
        Location location = mock(Location.class);
        when(location.getId()).thenReturn("imageLocationId");

        mockComputeServiceForListImages(location);

        cloudInfo = new CloudInfo("locationId", false, computeService);
        cloudInfo.showImages();

        verify(computeService).listImages();
    }

    private void mockComputeServiceForListHardware() {
        Hardware hardware = mock(Hardware.class);
        when(hardware.getId()).thenReturn("hardwareId");
        when(hardware.getLocation()).thenReturn(mock(Location.class));

        Set<? extends Hardware> hardwareSet = singleton(hardware);
        doReturn(hardwareSet).when(computeService).listHardwareProfiles();
    }

    private void mockComputeServiceForListImages(Location location) {
        Image image = mock(Image.class);
        when(image.getLocation()).thenReturn(location);

        Set<? extends Image> images = singleton(image);
        doReturn(images).when(computeService).listImages();
    }
}
