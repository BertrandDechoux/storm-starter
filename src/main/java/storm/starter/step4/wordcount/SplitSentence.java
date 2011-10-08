package storm.starter.step4.wordcount;

import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

public class SplitSentence extends ShellBolt implements IRichBolt {
	private static final long serialVersionUID = 1L;

	public SplitSentence() {
		super("python", "splitsentence.py");
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(getField()));
	}
	
	public String getField() {
		return "word";
	}
}