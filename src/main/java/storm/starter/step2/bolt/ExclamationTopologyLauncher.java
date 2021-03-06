package storm.starter.step2.bolt;

import static storm.starter.utils.ClusterHelper.aClusterWith;
import storm.starter.utils.TopologyBuilder;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;

public class ExclamationTopologyLauncher {

	public static void main(final String[] args) {
		final TopologyBuilder topology = new TopologyBuilder();
		
		final IRichSpout testWordSpout  = new TestWordSpout();
		final IRichBolt exclamationBolt1 = new ExclamationBolt();
		final IRichBolt exclamationBolt2 = new ExclamationBolt();

		topology.setSpout(testWordSpout, 10);
		topology.setBolt(exclamationBolt1, 3).shuffleGrouping(testWordSpout);
		topology.setBolt(exclamationBolt2, 2).shuffleGrouping(exclamationBolt1);

		aClusterWith(topology).debugAsLocal();
	}
	
}
