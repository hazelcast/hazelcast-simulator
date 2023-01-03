package com.hazelcast.simulator.tests.map.sql.realprod.client1;

import java.io.Serializable;
import java.util.Date;

public class Client1ModelClass1 implements Serializable {
    private Date date1;
    private long jobId;
    private Date date2;
    private Date date3;
    private int region;

    public Date getDate1() {
        return date1;
    }

    public void setDate1(Date date1) {
        this.date1 = date1;
    }

    public long getJobId() {
        return jobId;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public Date getDate2() {
        return date2;
    }

    public void setDate2(Date date2) {
        this.date2 = date2;
    }

    public Date getDate3() {
        return date3;
    }

    public void setDate3(Date date3) {
        this.date3 = date3;
    }

    public int getRegion() {
        return region;
    }

    public void setRegion(int region) {
        this.region = region;
    }
}
