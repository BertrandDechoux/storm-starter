package storm.starter.step2.spout;

import storm.starter.StormUtils;
import backtype.storm.topology.TopologyBuilder;

public class PrintTwitterSampleStream {
	
	public static void main(final String[] args) {
		final String username = args[0];
		final String password = args[1];

		final TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout(1, new TwitterSampleSpout(username, password));

		StormUtils.debugInLocalCluster(topologyBuilder);
	}
	
}
