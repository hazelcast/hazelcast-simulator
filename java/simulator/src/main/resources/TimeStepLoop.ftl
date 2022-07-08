import com.hazelcast.simulator.test.*;
import com.hazelcast.simulator.test.annotations.*;
import com.hazelcast.simulator.worker.testcontainer.*;
import com.hazelcast.simulator.worker.*;
import com.hazelcast.simulator.worker.metronome.*;
import com.hazelcast.simulator.probes.*;
import com.hazelcast.simulator.utils.*;

import org.apache.logging.log4j.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class ${className} extends TimeStepLoop {

<#if metronomeClass??>
</#if>
    public ${className}(${testInstanceClass} testInstance, TimeStepModel model, String executionGroup) {
        super(testInstance, model, executionGroup);
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
        final ${probeClass} ${method.name}Probe = (${probeClass})probeMap.get("${method.name}");
    </#list>
</#if>
<#if threadStateClass??>
        final ${threadStateClass} threadState = (${threadStateClass})this.threadState;
</#if>
<#if logFrequency??>
        long logCounter = 0;
</#if>
<#if logRateMs??>
        final ThrottlingLogger throttlingLogger = new ThrottlingLogger(logger, ${logRateMs});
</#if>

<#if timeStepMethods?size gt 1>
        final byte[] probs  = this.timeStepProbabilities;
</#if>

<#if hasIterationCap??>
        final long maxIterations = this.maxIterations;
</#if>

        long iteration = 0;
        while (!testContext.isStopped()) {
<#if probeClass??>
    <#if metronomeClass??>
            final long startNanos = metronome.waitForNext();
    <#else>
            final long startNanos = System.nanoTime();
    </#if>
<#else>
    <#if metronomeClass??>
            metronome.waitForNext();
    </#if>
</#if>

<#if timeStepMethods?size==1>
    <#assign method=timeStepMethods?first>
    <#assign resultType=method.getReturnType().getName()>
    <#if hasProbe(method)|| !probeClass?? || isAsyncResult(resultType)>
            <#assign resultName = "result">
            <#if isAsyncResult(resultType)>${resultType} ${resultName} = </#if><@timestepMethodCall m=method/>
            <#if isAsyncResult(resultType)>
                <@handleAsyncResult m=method/>
            </#if>
    <#else>
            <@timestepMethodCall m=method/>
            ${method.name}Probe.recordValue(System.nanoTime() - startNanos);
    </#if>
<#else>

            switch(probs[random.nextInt(probs.length)]){
    <#list timeStepMethods as method>
        <#assign index = method?counter-1>
                case ${index}:
        <#assign resultType=method.getReturnType().getName()>
        <#if hasProbe(method) || !probeClass?? || isAsyncResult(resultType)>
            <#assign resultName = "result" + index>
            <#if isAsyncResult(resultType)>
                    ${resultType} ${resultName} = </#if><@timestepMethodCall m=method/>
            <#if isAsyncResult(resultType)>
                    <@handleAsyncResult m=method/>
            </#if>
        <#else>
                    <@timestepMethodCall m=method/>
                    ${method.name}Probe.recordValue(System.nanoTime() - startNanos);
        </#if>
                    break;
    </#list>
            }
</#if>
            iteration++;
            iterations.lazySet(iteration);
<#if logFrequency??>
            logCounter++;
            if(logCounter == ${logFrequency}){
                logger.info("At " + logCounter);
                logCounter=0;
            }
</#if>
<#if logRateMs??>
            if(throttlingLogger.requestLogSlot()){
                throttlingLogger.logInSlot(Level.INFO, "At "+iteration);
            }
</#if>
<#if hasIterationCap??>
            if(iteration==maxIterations){
                break;
            }
</#if>
        }
    }

<#macro handleAsyncResult m>
                    ${resultName}.whenCompleteAsync((o, throwable) ->  ${m.getName()}Probe.recordValue(System.nanoTime() - startNanos), Runnable::run);
</#macro>

<#macro timestepMethodCall m>
    <@compress single_line=true>
       <#assign resultType=m.getReturnType().getName()>

       <#if resultType != "void" && !isAsyncResult(resultType)>
            <#if resultType == "boolean">
                   atomicBoolean.lazySet(
            <#elseif resultType == "byte" || resultType == "char" || resultType == "int" || resultType == "short">
                   atomicInteger.lazySet(
            <#elseif resultType == "long">
                   atomicLong.lazySet(
            <#elseif resultType == "double" || resultType == "float">
                   atomicLong.lazySet(Double.doubleToLongBits(
            <#else>
                   atomicReference.lazySet(
            </#if>
       </#if>

       testInstance.${m.getName()}(
        <#list  m.parameterTypes as param>
            <#if param?counter gt 1>,</#if>

            <#if isStartNanos(m, param?counter)>
                <#if metronomeClass??>startNanos<#else>System.nanoTime()</#if>
            <#elseif isAssignableFrom(param, Probe)>
                <#if probeClass??>${m.name}Probe<#else>com.hazelcast.simulator.probes.impl.EmptyProbe.INSTANCE</#if>
            <#else>
                threadState
            </#if>
        </#list>
     <#if resultType != "void" && !isAsyncResult(resultType)>
        <#if resultType == "double" || resultType == "float">)</#if>

     )
     </#if>
    );
    </@compress>
</#macro>
}