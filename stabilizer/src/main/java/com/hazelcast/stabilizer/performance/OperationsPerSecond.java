package com.hazelcast.stabilizer.performance;

public class OperationsPerSecond implements Performance<OperationsPerSecond> {

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
    public OperationsPerSecond merge(OperationsPerSecond that) {
        OperationsPerSecond performance = new OperationsPerSecond();
        performance.setStartMs(Math.min(that.getStartMs(), this.getStartMs()));
        performance.setEndMs(Math.max(that.getEndMs(), this.getEndMs()));
        performance.setOperations(this.getOperations() + that.getOperations());
        return performance;
    }

    @Override
    public String toHumanString() {
        long timeMs = endMs - startMs;
        double performance;
        if (timeMs == 0) {
            performance = 0;
        } else {
            performance = (operations * 1000d) / timeMs;
        }

        StringBuffer sb = new StringBuffer();
        sb.append("Performance = ").append(performance).append(" ops/s ");
        return sb.toString();
    }
}
