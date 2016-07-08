import com.hazelcast.simulator.test.*;
import com.hazelcast.simulator.test.annotations.*;
import com.hazelcast.simulator.worker.*;
import com.hazelcast.simulator.worker.tasks.*;
import com.hazelcast.simulator.worker.metronome.*;
import com.hazelcast.simulator.probes.*;
import java.util.*;
import java.util.concurrent.atomic.*;

public class GeneratedTimeStepTask_${id} extends TimeStepTask {

<#if metronomeClass??>
</#if>
    public GeneratedTimeStepTask_${id}(Object testInstance, TimeStepModel model) {
        super(testInstance, model);
    }

    @Override
    public void timeStepLoop() throws Exception {
<#if timeStepMethods?size gt 1>
        final Random random = new Random();
</#if>
        final AtomicLong iterations = this.iterations;
        final TestContextImpl testContext = (TestContextImpl)this.testContext;
        final ${testInstanceClass} testInstance = (${testInstanceClass})this.testInstance;
<#if metronomeClass??>
        final ${metronomeClass} metronome = (${metronomeClass})this.metronome;
</#if>
<#if probeClass??>
    <#list timeStepMethods as method>
        final ${probeClass} ${method.name}Probe = (${probeClass})this.probe;
    </#list>
</#if>
<#if threadContextClass??>
        final ${threadContextClass} threadContext = (${threadContextClass})this.threadContext;
</#if>

<#if timeStepMethods?size gt 1>
        final byte[] probs  = this.timeStepProbabilities;
</#if>

<#if probeClass??>
        long startNanos;
</#if>
        long iteration = 0;
        while (!testContext.isStopped()) {
<#if metronomeClass??>
            metronome.waitForNext();
</#if>
<#if timeStepMethods?size==1>
    <#assign method=timeStepMethods?first>
    <#if hasProbe(method)|| !probeClass??>
            <@timestepMethodCall m=method/>;
    <#else>
            startNanos = System.nanoTime();
            <@timestepMethodCall m=method/>;
            ${method.name}Probe.recordValue(System.nanoTime() - startNanos);
    </#if>
<#else>

            switch(probs[random.nextInt(probs.length)]){
    <#list timeStepMethods as method>
                case ${method?counter-1}:
        <#if hasProbe(method) || !probeClass??>
                    <@timestepMethodCall m=method/>;
        <#else>
                    startNanos = System.nanoTime();
                    <@timestepMethodCall m=method/>;
                    ${method.name}Probe.recordValue(System.nanoTime() - startNanos);
        </#if>
                    break;
    </#list>
            }
</#if>
            iteration++;
            iterations.lazySet(iteration);
        }
    }
<#macro timestepMethodCall m>
    <@compress single_line=true>
       testInstance.${m.getName()}(
        <#list  m.parameterTypes as param>
            <#if param?counter gt 1>,</#if>
            <@timestepMethodArg a=param m=m/>
        </#list>
    )
    </@compress>
</#macro>
<#macro timestepMethodArg a m>
    <#if isAssignableFrom(a, Probe)>
        ${m.name}Probe
    <#else>
        threadContext
    </#if>
</#macro>
}