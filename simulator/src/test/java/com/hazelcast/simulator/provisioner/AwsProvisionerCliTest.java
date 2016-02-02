package com.hazelcast.simulator.provisioner;

import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.helper.ExitStatusZeroException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.simulator.TestEnvironmentUtils.createAgentsFileWithLocalhost;
import static com.hazelcast.simulator.TestEnvironmentUtils.deleteAgentsFile;
import static com.hazelcast.simulator.TestEnvironmentUtils.resetSecurityManager;
import static com.hazelcast.simulator.TestEnvironmentUtils.resetUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.setDistributionUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.setExitExceptionSecurityManagerWithStatusZero;
import static com.hazelcast.simulator.provisioner.AwsProvisionerCli.init;
import static com.hazelcast.simulator.provisioner.AwsProvisionerCli.run;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class AwsProvisionerCliTest {

    private static File awsCredentials;

    private final List<String> args = new ArrayList<String>();

    private AwsProvisioner provisioner = mock(AwsProvisioner.class);

    @BeforeClass
    public static void setUp() {
        setExitExceptionSecurityManagerWithStatusZero();
        setDistributionUserDir();
        createAgentsFileWithLocalhost();
    }

    @AfterClass
    public static void tearDown() {
        resetSecurityManager();
        resetUserDir();
        deleteAgentsFile();
    }

    @Test
    public void testInit() {
        createAwsCredentialsFile();
        try {
            provisioner = init(getArgs());
        } finally {
            deleteQuiet(awsCredentials);
        }
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_withException() {
        provisioner = init(getArgs());
    }

    @Test(expected = ExitStatusZeroException.class)
    public void testRun_withoutArguments() {
        run(getArgs(), provisioner);
    }

    @Test(expected = ExitStatusZeroException.class)
    public void testRun_withHelp() {
        args.add("--help");
        run(getArgs(), provisioner);
    }

    @Test
    public void testRun_scale() {
        args.add("--scale");
        args.add("23");

        run(getArgs(), provisioner);

        verify(provisioner).scaleInstanceCountTo(eq(23));
        verify(provisioner).shutdown();
        verifyNoMoreInteractions(provisioner);
    }

    @Test
    public void testRun_newLoadBalancer() {
        args.add("--newLb");
        args.add("loadBalancerName");

        run(getArgs(), provisioner);

        verify(provisioner).createLoadBalancer(eq("loadBalancerName"));
        verify(provisioner).shutdown();
        verifyNoMoreInteractions(provisioner);
    }

    @Test
    public void testRun_addAgentsToLoadBalancer() {
        args.add("--addToLb");
        args.add("172.16.16.1");

        run(getArgs(), provisioner);

        verify(provisioner).addAgentsToLoadBalancer(eq("172.16.16.1"));
        verify(provisioner).shutdown();
        verifyNoMoreInteractions(provisioner);
    }

    private static void createAwsCredentialsFile() {
        awsCredentials = ensureExistingFile("awscredentials.properties");
        appendText("accessKey=foo" + NEW_LINE, awsCredentials);
        appendText("secretKey=bar" + NEW_LINE, awsCredentials);
    }

    private String[] getArgs() {
        String[] argsArray = new String[args.size()];
        args.toArray(argsArray);
        return argsArray;
    }
}
