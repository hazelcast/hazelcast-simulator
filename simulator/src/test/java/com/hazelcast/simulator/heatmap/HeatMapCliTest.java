package com.hazelcast.simulator.heatmap;

import com.hazelcast.simulator.utils.helper.ExitStatusZeroException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.simulator.TestEnvironmentUtils.deleteLogs;
import static com.hazelcast.simulator.TestEnvironmentUtils.resetSecurityManager;
import static com.hazelcast.simulator.TestEnvironmentUtils.resetUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.setDistributionUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.setExitExceptionSecurityManagerWithStatusZero;
import static com.hazelcast.simulator.heatmap.HeatMapCli.init;
import static com.hazelcast.simulator.heatmap.HeatMapCli.run;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class HeatMapCliTest {

    private final List<String> args = new ArrayList<String>();

    private HeatMap heatMap = mock(HeatMap.class);

    @BeforeClass
    public static void setUp() {
        setExitExceptionSecurityManagerWithStatusZero();
        setDistributionUserDir();
    }

    @AfterClass
    public static void tearDown() {
        resetSecurityManager();
        resetUserDir();
        deleteLogs();
    }

    @Test
    public void testInit() {
        init(getArgs());
    }

    @Test(expected = ExitStatusZeroException.class)
    public void testInit_withHelp() {
        args.add("--help");
        init(getArgs());
    }

    @Test
    public void testRun() {
        run(heatMap);

        verify(heatMap).createHeatMap();
        verify(heatMap).shutdown();
        verifyNoMoreInteractions(heatMap);
    }

    private String[] getArgs() {
        String[] argsArray = new String[args.size()];
        args.toArray(argsArray);
        return argsArray;
    }
}
