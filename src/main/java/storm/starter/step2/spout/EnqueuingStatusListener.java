package storm.starter.step2.spout;

import java.util.Queue;

import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.json.DataObjectFactory;

public class EnqueuingStatusListener implements StatusListener {
	private final Queue<String> queue;

	public EnqueuingStatusListener(final Queue<String> queue) {
		this.queue = queue;
	}

	@Override
	public void onStatus(final Status status) {
		queue.offer(DataObjectFactory.getRawJSON(status));
	}

	@Override
	public void onDeletionNotice(final StatusDeletionNotice sdn) {
	}

	@Override
	public void onTrackLimitationNotice(final int i) {
	}

	@Override
	public void onScrubGeo(final long l, final long l1) {
	}

	@Override
	public void onException(final Exception e) {
	}
}