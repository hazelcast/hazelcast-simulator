package com.hazelcast.heartattacker;

public class JavaInstallation {
    public String vendor;
    public String version;
    public String javaHome;

    public String getJavaHome() {
        return javaHome;
    }

    public void setJavaHome(String javaHome) {
        this.javaHome = javaHome;
    }

    public String getVendor() {
        return vendor;
    }

    public void setVendor(String vendor) {
        this.vendor = vendor;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @Override
    public String toString() {
        return "JavaInstallation{" +
                "javaHome='" + javaHome + '\'' +
                ", vendor='" + vendor + '\'' +
                ", version='" + version + '\'' +
                '}';
    }
}
