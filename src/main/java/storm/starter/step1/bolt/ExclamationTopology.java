package storm.starter.step1.bolt;

import storm.starter.StormUtils;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.TopologyBuilder;

public class ExclamationTopology {

	public static void main(final String[] args) {
		final TopologyBuilder topologyBuilder = new TopologyBuilder();

		topologyBuilder.setSpout(1, new TestWordSpout(), 10);
		topologyBuilder.setBolt(2, new ExclamationBolt(), 3).shuffleGrouping(1);
		topologyBuilder.setBolt(3, new ExclamationBolt(), 2).shuffleGrouping(2);

		StormUtils.debugInLocalCluster(topologyBuilder);
	}
	
}
