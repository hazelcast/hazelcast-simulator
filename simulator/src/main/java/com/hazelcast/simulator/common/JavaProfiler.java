package com.hazelcast.simulator.common;

/**
 * Defines the values for Java profiler property in {@value SimulatorProperties#PROPERTIES_FILE_NAME}.
 */
public enum JavaProfiler {

    NONE,
    YOURKIT,
    FLIGHTRECORDER,
    HPROF,
    PERF,
    VTUNE
}
