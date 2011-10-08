package storm.starter.step4.wordcount;

import static storm.starter.ClusterHelper.localClusterWith;
import storm.starter.TopologyBuilder;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichSpout;

public class WordCountTopology {
	public static void main(final String[] args) throws Exception {
		final TopologyBuilder topology = new TopologyBuilder();
		
		final IRichSpout randomSentenceSpout = new RandomSentenceSpout();
		final SplitSentence splitSentence = new SplitSentence();
		final IBasicBolt wordCount = new WordCount();

		topology.setSpout(randomSentenceSpout, 5);
		topology.setBolt(splitSentence, 8).shuffleGrouping(randomSentenceSpout);
		topology.setBolt(wordCount, 12).fieldsGrouping(splitSentence, splitSentence.getField());
		
		localClusterWith(topology).maxTaskParallelism(3).debug();

	}
}
