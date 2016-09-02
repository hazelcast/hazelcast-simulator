package com.hazelcast.simulator.heatmap;

import com.hazelcast.simulator.utils.helper.ExitStatusZeroException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.simulator.TestEnvironmentUtils.resetSecurityManager;
import static com.hazelcast.simulator.TestEnvironmentUtils.setExitExceptionSecurityManagerWithStatusZero;
import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.TestEnvironmentUtils.tearDownFakeEnvironment;
import static com.hazelcast.simulator.heatmap.HeatMapCli.init;
import static com.hazelcast.simulator.heatmap.HeatMapCli.run;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class HeatMapCliTest {

    private final List<String> args = new ArrayList<String>();

    private HeatMap heatMap = mock(HeatMap.class);

    @BeforeClass
    public static void beforeClass() {
        setExitExceptionSecurityManagerWithStatusZero();
        setupFakeEnvironment();
    }

    @AfterClass
    public static void afterClass() {
        resetSecurityManager();
        tearDownFakeEnvironment();
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
        verifyNoMoreInteractions(heatMap);
    }

    private String[] getArgs() {
        String[] argsArray = new String[args.size()];
        args.toArray(argsArray);
        return argsArray;
    }
}
