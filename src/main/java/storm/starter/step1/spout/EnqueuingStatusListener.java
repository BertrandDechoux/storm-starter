package storm.starter.step1.spout;

import java.util.Queue;

import org.apache.log4j.Logger;

import twitter4j.Status;
import twitter4j.StatusAdapter;
import twitter4j.json.DataObjectFactory;

public class EnqueuingStatusListener extends StatusAdapter {
	private static Logger LOGGER = Logger.getLogger(EnqueuingStatusListener.class);
	private final Queue<String> queue;

	public EnqueuingStatusListener(final Queue<String> queue) {
		this.queue = queue;
	}

	@Override
	public void onStatus(final Status status) {
		LOGGER.info(status.getUser().getName() + " : " + status.getText());
		queue.offer(DataObjectFactory.getRawJSON(status));
	}

}