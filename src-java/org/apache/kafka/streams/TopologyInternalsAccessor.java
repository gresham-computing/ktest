package org.apache.kafka.streams;

import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.streams.processor.internals.ProcessorTopologyAccessor;
import org.apache.kafka.streams.processor.internals.StreamTask;

import java.lang.invoke.MethodHandles;

public class TopologyInternalsAccessor {

	private TopologyInternalsAccessor() {
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

	public static StreamTask getTestStreamTask(TopologyTestDriver topologyTestDriver) {
		try {
			var handle = MethodHandles
					.privateLookupIn(TopologyTestDriver.class, MethodHandles.lookup())
					.findVarHandle(TopologyTestDriver.class, "task", StreamTask.class);

			return (StreamTask) handle.get(topologyTestDriver);
		} catch (Exception e){
			return null;
		}
	}

	public static void setTestStreamTask(TopologyTestDriver topologyTestDriver, StreamTask t) {
		try {
			var handle = MethodHandles
					.privateLookupIn(TopologyTestDriver.class, MethodHandles.lookup())
					.findVarHandle(TopologyTestDriver.class, "task", StreamTask.class);

			handle.set(topologyTestDriver, t);
		} catch (Exception e){
		}
	}

	public static boolean isRepartitionTopic(ProcessorTopology processorTopology, String topic) {
		return ProcessorTopologyAccessor.isRepartitionTopic(processorTopology, topic);
	}

}
