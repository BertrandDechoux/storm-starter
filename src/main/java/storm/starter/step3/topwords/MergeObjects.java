package storm.starter.step3.topwords;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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

	public static Logger LOG = Logger.getLogger(MergeObjects.class);

	private final List<List<Object>> _rankings = new ArrayList<List<Object>>();
	int _count = 10;
	Long _lastTime;

	public MergeObjects(final int n) {
		_count = n;
	}

	private int _compare(final List<Long> one, final List<Long> two) {
		return two.get(1).compareTo(one.get(1));
	}

	private Integer _find(final Object tag) {
		for (int i = 0; i < _rankings.size(); ++i) {
			final Object cur = _rankings.get(i).get(0);
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

		final List<List> merging = (List) JSONValue.parse(tuple.getString(0));
		for (final List pair : merging) {

			final Integer existingIndex = _find(pair.get(0));
			if (null != existingIndex) {
				_rankings.set(existingIndex, pair);
			} else {

				_rankings.add(pair);

			}

			Collections.sort(_rankings, new Comparator<List>() {
				@Override
				public int compare(final List o1, final List o2) {
					return _compare(o1, o2);
				}
			});

			if (_rankings.size() > _count) {
				_rankings.subList(_count, _rankings.size()).clear();
			}

		}

		final long currentTime = System.currentTimeMillis();
		if (_lastTime == null || currentTime >= _lastTime + 2000) {
			final String fullRankings = JSONValue.toJSONString(_rankings);
			collector.emit(new Values(fullRankings));
			LOG.info("Rankings: " + fullRankings);
			_lastTime = currentTime;
		}
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("list"));
	}

}
