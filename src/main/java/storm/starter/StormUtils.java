package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class StormUtils {

	public static void debugInLocalCluster(final TopologyBuilder topologyBuilder) {
		final Config conf = new Config();
		conf.setDebug(true);
		
		// you might want to use the interface ILocalCluster
		// but the implementation does not throw expected Exception
		final LocalCluster cluster = new LocalCluster();
		
		cluster.submitTopology("storm-starter-typologie", conf, topologyBuilder.createTopology());
		Utils.sleep(10000);
		cluster.killTopology("storm-starter-typologie");
		cluster.shutdown();
	}

}
