package storm.starter.step1.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ExclamationBolt implements IRichBolt {
	private static final long serialVersionUID = 1L;
	
	private OutputCollector collector;

	@Override
	public void prepare(final @SuppressWarnings("rawtypes") Map conf, final TopologyContext context,
			final OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(final Tuple tuple) {
		collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
		collector.ack(tuple);
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}