package storm.starter.step3.topwords;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class RollingCountObjects implements IRichBolt {
	private static final long serialVersionUID = 1L;

	private final HashMap<Object, long[]> _objectCounts = new HashMap<Object, long[]>();
	private final int _numBuckets;
	private transient Thread cleaner;
	private OutputCollector _collector;
	private final int _trackMinutes;

	public RollingCountObjects(final int numBuckets, final int trackMinutes) {
		_numBuckets = numBuckets;
		_trackMinutes = trackMinutes;
	}

	public long totalObjects(final Object obj) {
		final long[] curr = _objectCounts.get(obj);
		long total = 0;
		for (final long l : curr) {
			total += l;
		}
		return total;
	}

	public int currentBucket(final int buckets) {
		return (currentSecond() / secondsPerBucket(buckets)) % buckets;
	}

	public int currentSecond() {
		return (int) (System.currentTimeMillis() / 1000);
	}

	public int secondsPerBucket(final int buckets) {
		return (_trackMinutes * 60 / buckets);
	}

	public long millisPerBucket(final int buckets) {
		return (long) secondsPerBucket(buckets) * 1000;
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") final Map stormConf, final TopologyContext context,
			final OutputCollector collector) {
		_collector = collector;
		cleaner = new Thread(new Runnable() {
			@Override
			public void run() {
				Integer lastBucket = currentBucket(_numBuckets);

				while (true) {
					final int currBucket = currentBucket(_numBuckets);
					if (currBucket != lastBucket) {
						final int bucketToWipe = (currBucket + 1) % _numBuckets;
						synchronized (_objectCounts) {
							final Set<Object> objs = new HashSet<Object>(_objectCounts.keySet());
							for (final Object obj : objs) {
								final long[] counts = _objectCounts.get(obj);
								final long currBucketVal = counts[bucketToWipe];
								counts[bucketToWipe] = 0;
								final long total = totalObjects(obj);
								if (currBucketVal != 0) {
									_collector.emit(new Values(obj, total));
								}
								if (total == 0) {
									_objectCounts.remove(obj);
								}
							}
						}
						lastBucket = currBucket;
					}
					final long delta = millisPerBucket(_numBuckets)
							- (System.currentTimeMillis() % millisPerBucket(_numBuckets));
					Utils.sleep(delta);
				}
			}
		});
		cleaner.start();
	}

	@Override
	public void execute(final Tuple tuple) {

		final Object obj = tuple.getValue(0);
		final int bucket = currentBucket(_numBuckets);
		synchronized (_objectCounts) {
			long[] curr = _objectCounts.get(obj);
			if (curr == null) {
				curr = new long[_numBuckets];
				_objectCounts.put(obj, curr);
			}
			curr[bucket]++;
			_collector.emit(new Values(obj, totalObjects(obj)));
			_collector.ack(tuple);
		}
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("obj", "count"));
	}

}
