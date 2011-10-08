package storm.starter.step1.bolt;

import static storm.starter.ClusterHelper.localClusterWith;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.TopologyBuilder;

public class ExclamationTopology {

	public static void main(final String[] args) {
		final TopologyBuilder topology = new TopologyBuilder();
		
		final int sproutId = 1;
		final int bolt1Id = 2;
		final int bolt2Id = 3;

		topology.setSpout(sproutId, new TestWordSpout(), 10);
		topology.setBolt(bolt1Id, new ExclamationBolt(), 3).shuffleGrouping(sproutId);
		topology.setBolt(bolt2Id, new ExclamationBolt(), 2).shuffleGrouping(bolt1Id);

		localClusterWith(topology).debug();
	}
	
}
