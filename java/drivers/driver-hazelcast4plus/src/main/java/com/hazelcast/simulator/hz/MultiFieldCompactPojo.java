package com.hazelcast.simulator.hz;

public class MultiFieldCompactPojo {
    public String str1;
    public String str2;
    public String str3;
    public String str4;
    public String str5;

    public Integer int1;
    public Integer int2;
    public Integer int3;
    public Integer int4;
    public Integer int5;

    public Long long1;
    public Long long2;
    public Long long3;
    public Long long4;
    public Long long5;

    public Boolean bool1;
    public Boolean bool2;
    public Boolean bool3;
    public Boolean bool4;
    public Boolean bool5;

    public MultiFieldCompactPojo() {
    }

    public MultiFieldCompactPojo(
            String str1, String str2, String str3, String str4, String str5,
            Integer int1, Integer int2, Integer int3, Integer int4, Integer int5,
            Long long1, Long long2, Long long3, Long long4, Long long5,
            Boolean bool1, Boolean bool2, Boolean bool3, Boolean bool4, Boolean bool5
    ) {
        this.str1 = str1;
        this.str2 = str2;
        this.str3 = str3;
        this.str4 = str4;
        this.str5 = str5;
        this.int1 = int1;
        this.int2 = int2;
        this.int3 = int3;
        this.int4 = int4;
        this.int5 = int5;
        this.long1 = long1;
        this.long2 = long2;
        this.long3 = long3;
        this.long4 = long4;
        this.long5 = long5;
        this.bool1 = bool1;
        this.bool2 = bool2;
        this.bool3 = bool3;
        this.bool4 = bool4;
        this.bool5 = bool5;
    }
}
