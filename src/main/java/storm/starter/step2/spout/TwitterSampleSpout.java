package storm.starter.step2.spout;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TwitterSampleSpout implements IRichSpout {
	private static final long serialVersionUID = 1L;

	private final String username;
	private final String password;

	private SpoutOutputCollector collector;
	private Queue<String> queue = null;
	private TwitterStream twitterStream;

	public TwitterSampleSpout(final String username, final String password) {
		this.username = username;
		this.password = password;
	}

	@Override
	public boolean isDistributed() {
		return false;
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") final Map conf, final TopologyContext context,
			final SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<String>(1000);
		this.collector = collector;
		twitterStream = createTwitterStreamAndSample();
	}

	@Override
	public void nextTuple() {
		final String ret = queue.poll();
		if (ret == null) {
			Utils.sleep(50);
		} else {
			collector.emit(new Values(ret));
		}
	}

	@Override
	public void close() {
		twitterStream.shutdown();
	}

	@Override
	public void ack(final Object id) {
	}

	@Override
	public void fail(final Object id) {
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

	private TwitterStream createTwitterStreamAndSample() {
		final TwitterStream stream = new TwitterStreamFactory(//
				new ConfigurationBuilder().setUser(username).setPassword(password).build()//
		).getInstance();
		stream.addListener(new EnqueuingStatusListener(queue));
		stream.sample();
		return stream;
	}

}
