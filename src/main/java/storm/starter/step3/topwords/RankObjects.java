package storm.starter.step3.topwords;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONValue;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class RankObjects implements IBasicBolt {
	private static final long serialVersionUID = 1L;

	/**
	 * First object is the topic (word).
	 * Second object is the number it has been seen.
	 */
	private final List<List<?>> rankings = new ArrayList<List<?>>();
	private final int count;
	private final int millisecondOffest;
	private Long lastTime;

	public RankObjects(final int n, final int millisecondOffest) {
		count = n;
		this.millisecondOffest = millisecondOffest;
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") final Map stormConf, final TopologyContext context) {

	}

	@Override
	public void execute(final Tuple tuple, final BasicOutputCollector collector) {
		updateRankingsWithTuple(tuple);
		removeBottomRankingsIfNeeded();
		emitRankingsIfTimeHasElapsed(collector);
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("list"));
	}

	private Integer find(final Object tag) {
		for (int i = 0; i < rankings.size(); ++i) {
			final Object current = rankings.get(i).get(0);
			if (current.equals(tag)) {
				return i;
			}
		}
		return null;
	}

	private void updateRankingsWithTuple(final Tuple tuple) {
		final Object tag = tuple.getValue(0);
		final Integer existingIndex = find(tag);
		List<?> values = (List<?>)tuple.getValues();
		if (null != existingIndex) {
			rankings.set(existingIndex, values);
		} else {
			rankings.add(values);
		}
		Collections.sort(rankings, new RankingComparator());
	}

	private void removeBottomRankingsIfNeeded() {
		if (rankings.size() > count) {
			rankings.remove(count);
		}
	}

	private void emitRankingsIfTimeHasElapsed(final BasicOutputCollector collector) {
		final long currentTime = System.currentTimeMillis();
		if (lastTime == null || currentTime >= lastTime + millisecondOffest) {
			collector.emit(new Values(JSONValue.toJSONString(rankings)));
			lastTime = currentTime;
		}
	}

}
