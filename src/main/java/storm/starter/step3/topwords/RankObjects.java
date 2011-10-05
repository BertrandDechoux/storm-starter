package storm.starter.step3.topwords;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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

	List<List> _rankings = new ArrayList<List>();

	int _count;
	Long _lastTime = null;

	public RankObjects(final int n) {
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

		final Object tag = tuple.getValue(0);

		final Integer existingIndex = _find(tag);
		if (null != existingIndex) {
			_rankings.set(existingIndex, tuple.getValues());
		} else {

			_rankings.add(tuple.getValues());

		}

		Collections.sort(_rankings, new Comparator<List>() {
			@Override
			public int compare(final List o1, final List o2) {
				return _compare(o1, o2);
			}
		});

		if (_rankings.size() > _count) {
			_rankings.remove(_count);
		}

		final long currentTime = System.currentTimeMillis();
		if (_lastTime == null || currentTime >= _lastTime + 2000) {
			collector.emit(new Values(JSONValue.toJSONString(_rankings)));
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
