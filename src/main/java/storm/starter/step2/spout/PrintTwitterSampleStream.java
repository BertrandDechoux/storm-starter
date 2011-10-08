package storm.starter.step2.spout;

import static storm.starter.ClusterHelper.localClusterWith;
import backtype.storm.topology.TopologyBuilder;

public class PrintTwitterSampleStream {
	
	public static void main(final String[] args) {
		final String username = args[0];
		final String password = args[1];
		
		final int spoutId = 1;

		final TopologyBuilder topology = new TopologyBuilder();
		topology.setSpout(spoutId, new TwitterSampleSpout(username, password));

		localClusterWith(topology).debug();
	}
	
}
