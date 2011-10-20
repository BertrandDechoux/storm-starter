package storm.starter.step3.topology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.simple.JSONValue;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MergeObjects implements IBasicBolt {
	private static final long serialVersionUID = 1L;
	private static Logger LOGGER = Logger.getLogger(MergeObjects.class);

	private final List<List<?>> rankings = new ArrayList<List<?>>();
	private final int count;
	private final int millisecondOffset;
	private Long lastTime;

	public MergeObjects(final int n, final int millisecondOffset) {
		count = n;
		this.millisecondOffset = millisecondOffset;
	}

	private Integer find(final Object tag) {
		for (int i = 0; i < rankings.size(); ++i) {
			final Object cur = rankings.get(i).get(0);
			if (cur.equals(tag)) {
				return i;
			}
		}
		return null;

	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") final Map stormConf, final TopologyContext context) {

	}

	@Override
	public void execute(final Tuple tuple, final BasicOutputCollector collector) {
		@SuppressWarnings("unchecked")
		final List<List<?>> merging = (List<List<?>>) JSONValue.parse(tuple.getString(0));
		for (final List<?> pair : merging) {
			updateRankingsWithPair(pair);
			removeBottomRankingsIfNeeded();
		}
		emitRankingsIfTimeHasElapsed(collector);
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("list"));
	}

	private void updateRankingsWithPair(List<?> pair) {
		final Integer existingIndex = find(pair.get(0));
		if (null != existingIndex) {
			rankings.set(existingIndex, pair);
		} else {
			rankings.add(pair);
		}
		Collections.sort(rankings, new RankingComparator());
	}

	private void removeBottomRankingsIfNeeded() {
		if (rankings.size() > count) {
			rankings.subList(count, rankings.size()).clear();
		}
	}

	private void emitRankingsIfTimeHasElapsed(final BasicOutputCollector collector) {
		final long currentTime = System.currentTimeMillis();
		if (lastTime == null || currentTime >= lastTime + millisecondOffset) {
			final String fullRankings = JSONValue.toJSONString(rankings);
			collector.emit(new Values(fullRankings));
			LOGGER.info("Rankings: " + fullRankings);
			lastTime = currentTime;
		}
	}

}
