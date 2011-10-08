package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.utils.Utils;

public class ClusterHelper {
	public static ClusterHelper localClusterWith(final TopologyBuilder topology) {
		return new ClusterHelper(topology);
	}
	
	private final Config config = new Config();
	private final TopologyBuilder topologyBuilder;
	
	private ClusterHelper(final TopologyBuilder topologyBuilder) {
		this.topologyBuilder = topologyBuilder;
	}
	
	public ClusterHelper maxTaskParallelism(final int max) {
		config.setMaxTaskParallelism(3);
		return this;
	}
	
	public void debug() {
		config.setDebug(true);
		run();
	}
	
	public void run() {
		// you might want to use the interface ILocalCluster
		// but the interface declares throwing expected Exception
		final LocalCluster cluster = new LocalCluster();

		cluster.submitTopology("storm-starter-typologie", config, topologyBuilder.createTopology());
		Utils.sleep(10000);
		cluster.killTopology("storm-starter-typologie");
		cluster.shutdown();
	}

}
