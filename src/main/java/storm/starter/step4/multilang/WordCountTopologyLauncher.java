package storm.starter.step4.multilang;

import static storm.starter.utils.ClusterHelper.aClusterWith;
import storm.starter.utils.TopologyBuilder;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichSpout;

public class WordCountTopologyLauncher {
	public static void main(final String[] args) throws Exception {
		final TopologyBuilder topology = new TopologyBuilder();
		
		final IRichSpout randomSentenceSpout = new RandomSentenceSpout();
		final SplitSentence splitSentence = new SplitSentence();
		final IBasicBolt wordCount = new WordCount();

		topology.setSpout(randomSentenceSpout, 5);
		topology.setBolt(splitSentence, 8).shuffleGrouping(randomSentenceSpout);
		topology.setBolt(wordCount, 12).fieldsGrouping(splitSentence, splitSentence.getField());
		
		aClusterWith(topology).andMaxTaskParallelism(3).debugAsLocal();

	}
}
