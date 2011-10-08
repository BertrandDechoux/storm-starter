package storm.starter.step4.wordcount;


import static storm.starter.ClusterHelper.localClusterWith;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WordCountTopology {
	public static void main(final String[] args) throws Exception {
		final TopologyBuilder topology = new TopologyBuilder();
		
		final int spoutId = 1;
		final int splitSequenceId = 2;
		final int wordCountId = 3;

		topology.setSpout(spoutId, new RandomSentenceSpout(), 5);
		topology.setBolt(splitSequenceId, new SplitSentence(), 8).shuffleGrouping(spoutId);
		topology.setBolt(wordCountId, new WordCount(), 12).fieldsGrouping(splitSequenceId, new Fields("word"));
		
		localClusterWith(topology).maxTaskParallelism(3).debug();

	}
}
