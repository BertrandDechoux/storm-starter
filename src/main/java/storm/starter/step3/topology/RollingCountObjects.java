package storm.starter.step3.topology;

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
	
	private final HashMap<Object, long[]> objectCounts = new HashMap<Object, long[]>();
	private final int numBuckets;
	private transient Thread cleaner;
	private OutputCollector collector;
	private final int trackMinutes;

	public RollingCountObjects(final int numBuckets, final int trackMinutes) {
		this.numBuckets = numBuckets;
		this.trackMinutes = trackMinutes;
	}

	public long totalObjects(final Object obj) {
		final long[] curr = objectCounts.get(obj);
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
		return (trackMinutes * 60 / buckets);
	}

	public long millisPerBucket(final int buckets) {
		return (long) secondsPerBucket(buckets) * 1000;
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") final Map stormConf, final TopologyContext context,
			final OutputCollector collector) {
		this.collector = collector;
		cleaner = new Thread(new CleanerRunnable(collector));
		cleaner.start();
	}

	@Override
	public void execute(final Tuple tuple) {
		final Object obj = tuple.getValue(0);
		final int bucket = currentBucket(numBuckets);
		synchronized (objectCounts) {
			long[] curr = objectCounts.get(obj);
			if (curr == null) {
				curr = new long[numBuckets];
				objectCounts.put(obj, curr);
			}
			curr[bucket]++;
			collector.emit(new Values(obj, totalObjects(obj)));
			collector.ack(tuple);
		}
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(getField(), "count"));
	}

	public String getField() {
		return "obj";
	}

	private final class CleanerRunnable implements Runnable {
		private final OutputCollector collector;
	
		private CleanerRunnable(OutputCollector collector) {
			this.collector = collector;
		}
	
		@Override
		public void run() {
			Integer lastBucket = currentBucket(numBuckets);
	
			while (true) {
				final int currBucket = currentBucket(numBuckets);
				if (currBucket != lastBucket) {
					final int bucketToWipe = (currBucket + 1) % numBuckets;
					synchronized (objectCounts) {
						final Set<Object> objs = new HashSet<Object>(objectCounts.keySet());
						for (final Object obj : objs) {
							final long[] counts = objectCounts.get(obj);
							final long currBucketVal = counts[bucketToWipe];
							counts[bucketToWipe] = 0;
							final long total = totalObjects(obj);
							if (currBucketVal != 0) {
								collector.emit(new Values(obj, total));
							}
							if (total == 0) {
								objectCounts.remove(obj);
							}
						}
					}
					lastBucket = currBucket;
				}
				final long delta = millisPerBucket(numBuckets)
						- (System.currentTimeMillis() % millisPerBucket(numBuckets));
				Utils.sleep(delta);
			}
		}
	}

}
