package com.hazelcast.heartattacker.performance;

public class OperationsPerSecond implements Performance{

    private long operations;
    private long startMs;
    private long endMs;

    public long getOperations() {
        return operations;
    }

    public void setOperations(long operations) {
        this.operations = operations;
    }

    public long getStartMs() {
        return startMs;
    }

    public void setStartMs(long startMs) {
        this.startMs = startMs;
    }

    public long getEndMs() {
        return endMs;
    }

    public void setEndMs(long endMs) {
        this.endMs = endMs;
    }

    @Override
    public String toHumanString() {
        long timeMs = endMs-startMs;
        double performance;
        if(timeMs == 0){
             performance = 0;
        }else{
            performance = (operations*1000d)/timeMs;
        }

        StringBuffer sb = new StringBuffer();
        sb.append("Performance = ").append(performance).append(" operations/second ");
        return sb.toString();
    }
}
