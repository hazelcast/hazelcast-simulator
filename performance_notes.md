# A selection of performance notes.

This document contains some useful performance notes.

# Async-Profiler

Install async profiler;

```
inventory install async_profiler
```

Make sure you have added following JVM settings:
```
-XX:+UnlockDiagnosticVMOptions
-XX:+DebugNonSafepoints
```

And to run, execute the following if you want a jfr file which can be analyzed with the Java Mission Control:
```
profiler.sh collect -d 60 --jfrsync  ${JAVA_HOME}/lib/jfr/default.jfc  -f profile.jfr Worker
```

Use the following for more accurate profiling:
```
profiler.sh collect -d 60 --jfrsync  ${JAVA_HOME}/lib/jfr/default.jfc  -e cycles --cstack lbr  -f profile.jfr Worker
```

Or use the following if you do not care for a jfr file but want a flamegraph directly:
```
profiler.sh collect -d 60 -f flamegraph.html Worker
```

# Perf

Make sure you have added following JVM settings:
```
-XX:+UnlockDiagnosticVMOptions
-XX:+DebugNonSafepoints
-XX:+PreserveFramePointer
```

Creating a perfmap with java 17+
```
jcmd <pid> Compiler.perfmap
```

https://www.doof.me.uk/2021/02/28/generating-perf-maps-with-openjdk-17/

# JFR


```
-XX:+UnlockDiagnosticVMOptions
-XX:+DebugNonSafepoints
-XX:+PreserveFramePointer
-XX:+FlightRecorder
-XX:StartFlightRecording=duration=3600s,filename=recording.jfr
```


# Page Cache

Clear PageCache only.

```
sync
echo 1 | sudo tee /proc/sys/vm/drop_caches
```

Clear dentries and inodes.

```
sync
echo 2 | sudo tee /proc/sys/vm/drop_caches
```

Clear pagecache, dentries, and inodes.
```
sync
echo 3 | sudo tee /proc/sys/vm/drop_caches
```

