package com.hazelcast.simulator.utils;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;

import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;

public final class NativeUtils {

    private static final Logger LOGGER = Logger.getLogger(NativeUtils.class);

    private NativeUtils() {
    }

    //see http://stackoverflow.com/questions/35842/how-can-a-java-program-get-its-own-process-id
    public static Integer getPIDorNull() {
        Integer pidFromManagementBean = getPidFromManagementBean();
        return pidFromManagementBean != null ? pidFromManagementBean : getPidViaReflection();
    }

    public static void kill(int pid) {
        LOGGER.info("Sending -9 signal to PID " + pid);
        try {
            Runtime.getRuntime().exec("/bin/kill -9 " + pid + " >/dev/null");
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static void execute(String command) {
        StringBuffer sb = new StringBuffer();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Executing bash command: " + command);
        }

        try {
            // create a process for the shell
            ProcessBuilder pb = new ProcessBuilder("bash", "-c", command);
            pb = pb.redirectErrorStream(true);

            Process shell = pb.start();
            new BashStreamGobbler(shell.getInputStream(), sb).start();

            // wait for the shell to finish and get the return code
            int shellExitStatus = shell.waitFor();

            if (shellExitStatus != 0) {
                LOGGER.error(String.format("Failed to execute [%s]", command));
                LOGGER.error(sb.toString());
                System.exit(1);
            } else {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Bash output: \n" + sb);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static Integer getPidFromManagementBean() {
        String name = ManagementFactory.getRuntimeMXBean().getName();
        int indexOf = name.indexOf("@");
        if (indexOf == -1) {
            return null;
        }
        String pidString = name.substring(0, indexOf);
        try {
            return Integer.parseInt(pidString);
        } catch (NumberFormatException e) {
            LOGGER.warn(e);
            return null;
        }
    }

    private static Integer getPidViaReflection() {
        try {
            java.lang.management.RuntimeMXBean runtime = java.lang.management.ManagementFactory.getRuntimeMXBean();
            java.lang.reflect.Field jvm = runtime.getClass().getDeclaredField("jvm");
            jvm.setAccessible(true);
            sun.management.VMManagement mgmt = (sun.management.VMManagement) jvm.get(runtime);
            java.lang.reflect.Method pid_method = mgmt.getClass().getDeclaredMethod("getProcessId");
            pid_method.setAccessible(true);
            return (Integer) pid_method.invoke(mgmt);
        } catch (IllegalAccessException e) {
            LOGGER.warn(e);
            return null;
        } catch (InvocationTargetException e) {
            LOGGER.warn(e);
            return null;
        } catch (NoSuchMethodException e) {
            LOGGER.warn(e);
            return null;
        } catch (NoSuchFieldException e) {
            LOGGER.warn(e);
            return null;
        }
    }

    public static class BashStreamGobbler extends Thread {
        private final BufferedReader reader;
        private final StringBuffer stringBuffer;

        public BashStreamGobbler(InputStream in, StringBuffer stringBuffer) {
            this.reader = new BufferedReader(new InputStreamReader(in));
            this.stringBuffer = stringBuffer;
        }

        @Override
        public void run() {
            try {
                String line;
                while ((line = reader.readLine()) != null) {
                    stringBuffer.append(line).append("\n");
                }
            } catch (IOException ioException) {
                //log.warn("System command stream gobbler error", ioException);
            } finally {
                closeQuietly(reader);
            }
        }
    }
}
