package storm.starter.step4.wordcount;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.ShellBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordCountTopology {
	public static class SplitSentence extends ShellBolt implements IRichBolt {
		private static final long serialVersionUID = 1L;

		public SplitSentence() {
			super("python", "splitsentence.py");
		}

		@Override
		public void declareOutputFields(final OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));
		}
	}

	public static class WordCount implements IBasicBolt {
		private static final long serialVersionUID = 1L;
		Map<String, Integer> counts = new HashMap<String, Integer>();

		@Override
		public void prepare(@SuppressWarnings("rawtypes") final Map conf, final TopologyContext context) {
		}

		@Override
		public void execute(final Tuple tuple, final BasicOutputCollector collector) {
			final String word = tuple.getString(0);
			Integer count = counts.get(word);
			if (count == null)
				count = 0;
			count++;
			counts.put(word, count);
			collector.emit(new Values(word, count));
		}

		@Override
		public void cleanup() {
		}

		@Override
		public void declareOutputFields(final OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word", "count"));
		}
	}

	public static void main(final String[] args) throws Exception {

		final TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout(1, new RandomSentenceSpout(), 5);

		builder.setBolt(2, new SplitSentence(), 8).shuffleGrouping(1);
		builder.setBolt(3, new WordCount(), 12).fieldsGrouping(2, new Fields("word"));

		final Config conf = new Config();
		conf.setDebug(true);
		conf.setMaxTaskParallelism(3);

		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("word-count", conf, builder.createTopology());
		Thread.sleep(10000);

		cluster.shutdown();

	}
}
