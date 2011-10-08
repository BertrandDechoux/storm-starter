package storm.starter.step2.spout;

import java.util.Queue;

import twitter4j.Status;
import twitter4j.StatusAdapter;
import twitter4j.json.DataObjectFactory;

public class EnqueuingStatusListener extends StatusAdapter {
	private final Queue<String> queue;

	public EnqueuingStatusListener(final Queue<String> queue) {
		this.queue = queue;
	}

	@Override
	public void onStatus(final Status status) {
		queue.offer(DataObjectFactory.getRawJSON(status));
	}

}