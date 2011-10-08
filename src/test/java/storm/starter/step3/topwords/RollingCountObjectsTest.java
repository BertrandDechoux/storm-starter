package storm.starter.step3.topwords;

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.tuple.Tuple;

import static org.mockito.Mockito.*;

/**
 * TODO test rolling
 */
@RunWith(MockitoJUnitRunner.class)
public class RollingCountObjectsTest {
	private static final int NUM_BUCKETS = 1;
	private static final int TRACK_MINUTES = 1;
	private IRichBolt rollingCountObjects = new RollingCountObjects(NUM_BUCKETS, TRACK_MINUTES);
	
	@Mock
	private Map<Object, Object> stormConf;
	@Mock
	private TopologyContext topologyContext;
	@Mock
	private OutputCollector collector;
	@Mock
	private Tuple input;
	
	@SuppressWarnings("rawtypes")
	private final ArgumentCaptor<List> values = ArgumentCaptor.forClass(List.class);

	@Test
	public void shouldCount1ForFirst() {
		rollingCountObjects.prepare(stormConf, topologyContext, collector);

		when(input.getValue(0)).thenReturn("boltInput");
		rollingCountObjects.execute(input);

		verifyEmit("boltInput", 1);
		verify(collector).ack(input);
	}
	
	@Test
	@SuppressWarnings("unchecked")
	public void shouldCount2ForSecond() {
		rollingCountObjects.prepare(stormConf, topologyContext, collector);

		when(input.getValue(0)).thenReturn("boltInput");
		rollingCountObjects.execute(input);
		rollingCountObjects.execute(input);

		verify(collector, times(2)).emit(values.capture());
		verify(collector, times(2)).ack(input);
		
		assertEmitted(0,"boltInput", 1);
		assertEmitted(1,"boltInput", 2);		
	}
	
	@Test
	@SuppressWarnings("unchecked")
	public void shouldCountForTwoDifferentObjects() {
		rollingCountObjects.prepare(stormConf, topologyContext, collector);

		when(input.getValue(0)).thenReturn("boltInput1","boltInput2","boltInput1");
		rollingCountObjects.execute(input);
		rollingCountObjects.execute(input);
		rollingCountObjects.execute(input);

		verify(collector, times(3)).emit(values.capture());
		verify(collector, times(3)).ack(input);
		
		assertEmitted(0,"boltInput1", 1);
		assertEmitted(1,"boltInput2", 1);
		assertEmitted(2,"boltInput1", 2);	
	}

	@SuppressWarnings("unchecked")
	private void verifyEmit(final Object object, final long total) {
		verify(collector).emit(values.capture());
		assertThat(values.getValue()).containsExactly(object, total);
	}
	
	private void assertEmitted(final int index, final Object object, final long total) {
		assertThat(values.getAllValues().get(index)).containsExactly(object, total);
	}
}
