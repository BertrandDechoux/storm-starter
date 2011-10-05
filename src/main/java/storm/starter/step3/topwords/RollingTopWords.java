package storm.starter.step3.topwords;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class RollingTopWords {

	public static void main(final String[] args) throws Exception {

		final int TOP_N = 3;

		final TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout(1, new TestWordSpout(), 5);

		builder.setBolt(2, new RollingCountObjects(60, 10), 4).fieldsGrouping(1, new Fields("word"));
		builder.setBolt(3, new RankObjects(TOP_N), 4).fieldsGrouping(2, new Fields("obj"));
		builder.setBolt(4, new MergeObjects(TOP_N)).globalGrouping(3);

		final Config conf = new Config();
		conf.setDebug(true);

		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("rolling-demo", conf, builder.createTopology());
		Thread.sleep(10000);

		cluster.shutdown();

	}
}
