package storm.starter.step2.bolt;

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import storm.starter.step2.bolt.ExclamationBolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.tuple.Tuple;

@RunWith(MockitoJUnitRunner.class)
public class ExclamationBoltTest {
	private IRichBolt exclamationBolt = new ExclamationBolt();

	@Mock
	private Map<Object, Object> stormConf;
	@Mock
	private TopologyContext topologyContext;
	@Mock
	private OutputCollector collector;
	@Mock
	private Tuple input;

	@Test
	public void shouldAddExclamationarksToInput() {
		exclamationBolt.prepare(stormConf, topologyContext, collector);

		when(input.getString(0)).thenReturn("boltInput");
		exclamationBolt.execute(input);

		verifyEmit("boltInput!!!");
		verify(collector).ack(input);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void verifyEmit(String emittedString) {
		final ArgumentCaptor<List> values = ArgumentCaptor.forClass(List.class);
		verify(collector).emit(eq(input), values.capture());
		assertThat(values.getValue()).containsExactly(emittedString);
	}
}
