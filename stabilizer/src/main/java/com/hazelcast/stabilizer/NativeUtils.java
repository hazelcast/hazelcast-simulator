package com.hazelcast.stabilizer;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;

//see http://stackoverflow.com/questions/35842/how-can-a-java-program-get-its-own-process-id
public class NativeUtils {
    private static final Logger log = Logger.getLogger(NativeUtils.class);

    private NativeUtils() { }

    public static Integer getPIDorNull() {
        Integer pidFromManagementBean = getPidFromManagementBean();
        return pidFromManagementBean != null ? pidFromManagementBean : getPidViaReflection();
    }

    public static void kill(int pid) {
        log.info("Sending -9 signal to PID "+pid);
        try {
            Runtime.getRuntime().exec("/bin/kill -9 "+pid+" >/dev/null");
        } catch (IOException e) {
            throw new IllegalStateException(e);
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
            log.warn(e);
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
            return  (Integer) pid_method.invoke(mgmt);
        } catch (IllegalAccessException e) {
            log.warn(e);
            return null;
        } catch (InvocationTargetException e) {
            log.warn(e);
            return null;
        } catch (NoSuchMethodException e) {
            log.warn(e);
            return null;
        } catch (NoSuchFieldException e) {
            log.warn(e);
            return null;
        }
    }
}
