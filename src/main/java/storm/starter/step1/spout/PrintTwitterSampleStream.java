package storm.starter.step1.spout;

import static storm.starter.ClusterHelper.localClusterWith;
import storm.starter.TopologyBuilder;
import backtype.storm.topology.IRichSpout;

public class PrintTwitterSampleStream {

	public static void main(final String[] args) {
		final String username = args[0];
		final String password = args[1];

		final IRichSpout twitterSampleSpout = new TwitterSampleSpout(username, password);

		final TopologyBuilder topology = new TopologyBuilder();
		topology.setSpout(twitterSampleSpout);

		localClusterWith(topology).debug();
	}

}
