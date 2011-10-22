package storm.starter.step1.spout;

import static storm.starter.utils.ClusterHelper.aClusterWith;
import storm.starter.utils.TopologyBuilder;

public class PrintTwitterSampleStreamLauncher {

	public static void main(final String[] args) {
		final String username = args[0];
		final String password = args[1];

		final TopologyBuilder topology = new TopologyBuilder();
		topology.setSpout(new TwitterSampleSpout(username, password));

		aClusterWith(topology).debugAsLocal();
	}

}
