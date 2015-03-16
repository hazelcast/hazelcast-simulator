package com.hazelcast.simulator.probes.probes;

public enum ProbesResultXmlElements {

    PROBES_RESULT("probes-result"),
    PROBE("probe"),
    PROBE_NAME("name"),
    PROBE_TYPE("type"),

    OPERATIONS_PER_SECOND("operations-per-second"),

    MAX_LATENCY("max-latency"),

    HDR_LATENCY_DATA("data"),

    LATENCY_DIST_STEP("step"),
    LATENCY_DIST_MAX_VALUE("max-value"),
    LATENCY_DIST_BUCKETS("buckets"),
    LATENCY_DIST_BUCKET("bucket"),
    LATENCY_DIST_UPPER_BOUND("upper-bound"),
    LATENCY_DIST_VALUES("values");

    public final String string;

    ProbesResultXmlElements(String string) {
        this.string = string;
    }
}
