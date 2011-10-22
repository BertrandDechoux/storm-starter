package storm.starter.utils;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.Utils;

/**
 * A limited pseudo fluent api for running a {@link StormTopology} inside a
 * cluster for test purpose.
 */
public final class ClusterHelper {
	public static ClusterHelper aClusterWith(final TopologyBuilder topology) {
		return new ClusterHelper(topology);
	}

	private final Config config = new Config();
	private final TopologyBuilder topologyBuilder;
	private String topologyName = "storm-starter-typology";

	private ClusterHelper(final TopologyBuilder topologyBuilder) {
		this.topologyBuilder = topologyBuilder;
	}

	public ClusterHelper andMaxTaskParallelism(final int max) {
		config.setMaxTaskParallelism(3);
		return this;
	}

	public ClusterHelper andTopologyName(final String topologyName) {
		this.topologyName = topologyName;
		return this;
	}

	public void debugAsLocal() {
		config.setDebug(true);
		runAsLocal();
	}

	public void runAsLocal() {
		// you might want to use the interface ILocalCluster
		// but the interface declares throwing expected Exception
		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(topologyName, config,
				topologyBuilder.createTopology());
		Utils.sleep(10000);
		cluster.killTopology(topologyName);
		cluster.shutdown();
	}

	public void run() {
		try {
			config.setNumWorkers(3);
			StormSubmitter.submitTopology(topologyName, config,
					topologyBuilder.createTopology());
		} catch (final Exception e) {
			// don't do that at home!
			throw new IllegalStateException(e);
		}
	}

}
