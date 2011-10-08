package storm.starter.step3.topwords;

import static storm.starter.ClusterHelper.localClusterWith;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class RollingTopWords {
	private static final int TOP_N = 3;
	private static final int MILLISECONDS_OFFEST = 2000;
	private static final String SPOUT_FIELD = "word";

	public static void main(final String[] args) throws Exception {
		final TopologyBuilder topology = new TopologyBuilder();
		
		final int spoutId = 1;
		final int rollingCountBoltId = 2;
		final int rankBoltId = 3;
		final int mergeBoltId = 4;
		
		topology.setSpout(spoutId, new TestWordSpout(), 5);

		topology.setBolt(rollingCountBoltId, new RollingCountObjects(60, 10), 4).fieldsGrouping(spoutId, new Fields(SPOUT_FIELD));
		topology.setBolt(rankBoltId, new RankObjects(TOP_N,MILLISECONDS_OFFEST) , 4).fieldsGrouping(rollingCountBoltId, new Fields(RollingCountObjects.OBJ_FIELD));
		topology.setBolt(mergeBoltId, new MergeObjects(TOP_N,MILLISECONDS_OFFEST)).globalGrouping(rankBoltId);
		
		localClusterWith(topology).run();
	}
}
