package org.apache.kafka.streams;

import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.streams.processor.internals.ProcessorTopologyAccessor;
import org.apache.kafka.streams.processor.internals.StreamTask;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public class TopologyInternalsAccessor {

    private VarHandle handle;

    public TopologyInternalsAccessor() throws NoSuchFieldException, IllegalAccessException {
        this.handle = MethodHandles
                .privateLookupIn(TopologyTestDriver.class, MethodHandles.lookup())
                .findVarHandle(TopologyTestDriver.class, "task", StreamTask.class);
    }

    public static InternalTopologyBuilder internalTopologyBuilder(Topology topology) {
        return topology.internalTopologyBuilder;
    }

    public static ProcessorTopology processorTopology(TopologyTestDriver topologyTestDriver) {
        return topologyTestDriver.processorTopology;
    }

    public static ProcessorTopology globalProcessorTopology(TopologyTestDriver topologyTestDriver) {
        return topologyTestDriver.globalTopology;
    }

    public StreamTask getTestStreamTask(TopologyTestDriver topologyTestDriver) {
        return (StreamTask) this.handle.get(topologyTestDriver);
    }

    public void setTestStreamTask(TopologyTestDriver topologyTestDriver, StreamTask t) {
        this.handle.set(topologyTestDriver, t);
    }

    public static boolean isRepartitionTopic(ProcessorTopology processorTopology, String topic) {
        return ProcessorTopologyAccessor.isRepartitionTopic(processorTopology, topic);
    }

}
