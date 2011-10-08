package storm.starter.step3.topwords;

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Lists;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.tuple.Tuple;

@RunWith(MockitoJUnitRunner.class)
public class MergeObjectsTest {
	private static final int TOP_N = 3;
	private static final int MILLISECONDS_OFFEST = 0;
	private IBasicBolt mergeObjects = new MergeObjects(TOP_N, MILLISECONDS_OFFEST);

	@Mock
	private Map<Object, Object> stormConf;
	@Mock
	private TopologyContext context;
	@Mock
	private BasicOutputCollector collector;
	@Mock
	private Tuple input;

	@SuppressWarnings("rawtypes")
	private final ArgumentCaptor<List> values = ArgumentCaptor.forClass(List.class);
	
	@Test
	@SuppressWarnings("unchecked")
	public void shouldMergeInputs() {
		mergeObjects.prepare(stormConf, context);

		when(input.getString(0)).thenReturn(//
				"[[\"alpha\",5]]",//
				"[[\"b\",6]]",//
				"[[\"gama\",4]]",//
				"[[\"t\",6]]");

		mergeObjects.execute(input, collector);
		mergeObjects.execute(input, collector);
		mergeObjects.execute(input, collector);
		mergeObjects.execute(input, collector);

		verify(collector, times(4)).emit(values.capture());

		assertEmitted(0, "[[\"alpha\",5]]");
		assertEmitted(1, "[[\"b\",6],[\"alpha\",5]]");
		assertEmitted(2, "[[\"b\",6],[\"alpha\",5],[\"gama\",4]]");
		assertEmitted(3, "[[\"b\",6],[\"t\",6],[\"alpha\",5]]");
	}

	private void assertEmitted(final int index, final String json) {
		assertThat(values.getAllValues().get(index)).containsExactly(json);
	}
	

}
