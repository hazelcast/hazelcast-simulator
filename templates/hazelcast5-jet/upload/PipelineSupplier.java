import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PipelineSupplier implements
        // Mandatory implementation for configuring the jobs
        Supplier<Map<JobConfig, Pipeline>>,
        // Optional implementation for controlling how to wait for completion
        BiConsumer<HazelcastInstance, List<Job>> {

    @Override
    public Map<JobConfig, Pipeline> get() {
        Pipeline p = Pipeline.create();
        List<String> items = IntStream.range(0, 1000).mapToObj(n -> UUID.randomUUID().toString()).collect(Collectors.toList());
        p.readFrom(TestSources.items(items)).writeTo(Sinks.logger());
        return Map.of(new JobConfig().setName("MyJobby"), p);
    }

    @Override
    public void accept(HazelcastInstance instance, List<Job> jobs) {
        jobs.forEach(Job::join);
    }
}
