/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.heartattacker;

import java.io.Serializable;

public class TraineeVmSettings implements Serializable {
    private String vmOptions;
    private boolean trackLogging;
    private String hzConfig;
    private int traineeCount;
    private int traineeStartupTimeout;
    private boolean refreshJvm;
    private String javaVendor;
    private String javaVersion;

    public String getJavaVendor() {
        return javaVendor;
    }

    public void setJavaVendor(String javaVendor) {
        this.javaVendor = javaVendor;
    }

    public String getJavaVersion() {
        return javaVersion;
    }

    public void setJavaVersion(String javaVersion) {
        this.javaVersion = javaVersion;
    }

    public int getTraineeStartupTimeout() {
        return traineeStartupTimeout;
    }

    public void setTraineeStartupTimeout(int traineeStartupTimeout) {
        this.traineeStartupTimeout = traineeStartupTimeout;
    }

    public boolean isRefreshJvm() {
        return refreshJvm;
    }

    public void setRefreshJvm(boolean refreshJvm) {
        this.refreshJvm = refreshJvm;
    }

    public String getHzConfig() {
        return hzConfig;
    }

    public void setHzConfig(String hzConfig) {
        this.hzConfig = hzConfig;
    }

    public boolean isTrackLogging() {
        return trackLogging;
    }

    public void setTrackLogging(boolean trackLogging) {
        this.trackLogging = trackLogging;
    }

    public int getTraineeCount() {
        return traineeCount;
    }

    public void setTraineeCount(int traineeCount) {
        this.traineeCount = traineeCount;
    }

    public String getVmOptions() {
        return vmOptions;
    }

    public void setVmOptions(String vmOptions) {
        this.vmOptions = vmOptions;
    }

    @Override
    public String toString() {
        return "TraineeSettings{" +
                "\n  vmOptions='" + vmOptions + '\'' +
                "\n, trackLogging=" + trackLogging +
                "\n, traineeVmCount=" + traineeCount +
                "\n, traineeStartupTimeout=" + traineeStartupTimeout +
                "\n, refreshJvm=" + refreshJvm +
                "\n, javaVendor=" + javaVendor +
                "\n, javaVersion=" + javaVersion +
                "\n, hzConfig='" + hzConfig + '\'' +
                "\n}";
    }
}
