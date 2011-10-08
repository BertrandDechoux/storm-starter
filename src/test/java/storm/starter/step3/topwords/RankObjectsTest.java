package storm.starter.step3.topwords;

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

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class RankObjectsTest {
	private static final int TOP_N = 3;
	private static final int MILLISECONDS_OFFEST = 0;
	private IBasicBolt rankObjects = new RankObjects(TOP_N, MILLISECONDS_OFFEST);

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
	public void shouldOuputSingleResult() {
		rankObjects.prepare(stormConf, context);

		when(input.getValue(0)).thenReturn("myInput");
		when(input.getValues()).thenReturn(newRanking("myInput", 5));
		rankObjects.execute(input, collector);

		verifyEmit("[[\"myInput\",5]]");
	}

	@Test
	@SuppressWarnings("unchecked")
	public void shouldOverwriteRankingForSameObject() {
		rankObjects.prepare(stormConf, context);

		when(input.getValue(0)).thenReturn("myInput");
		when(input.getValues()).thenReturn(//
				newRanking("myInput", 5),//
				newRanking("myInput", 6),//
				newRanking("myInput", 7));

		rankObjects.execute(input, collector);
		rankObjects.execute(input, collector);
		rankObjects.execute(input, collector);

		verify(collector, times(3)).emit(values.capture());

		assertEmitted(0, "[[\"myInput\",5]]");
		assertEmitted(1, "[[\"myInput\",6]]");
		assertEmitted(2, "[[\"myInput\",7]]");
	}

	@Test
	@SuppressWarnings("unchecked")
	public void shouldOnlyKeepTopNRankings() {
		rankObjects.prepare(stormConf, context);

		when(input.getValue(0)).thenReturn("alpha","beta","gama","omega");
		when(input.getValues()).thenReturn(//
				newRanking("alpha", 5),//
				newRanking("beta", 6),//
				newRanking("gama", 4),//
				newRanking("omega", 6));

		rankObjects.execute(input, collector);
		rankObjects.execute(input, collector);
		rankObjects.execute(input, collector);
		rankObjects.execute(input, collector);

		verify(collector, times(4)).emit(values.capture());

		assertEmitted(0, "[[\"alpha\",5]]");
		assertEmitted(1, "[[\"beta\",6],[\"alpha\",5]]");
		assertEmitted(2, "[[\"beta\",6],[\"alpha\",5],[\"gama\",4]]");
		assertEmitted(3, "[[\"beta\",6],[\"omega\",6],[\"alpha\",5]]");
	}
	
	private List<Object> newRanking(final String word, final long total) {
		return Lists.<Object> newArrayList(word, total);
	}

	@SuppressWarnings("unchecked")
	private void verifyEmit(final String json) {
		verify(collector).emit(values.capture());
		assertThat(values.getValue()).containsExactly(json);
	}

	private void assertEmitted(final int index, final String json) {
		assertThat(values.getAllValues().get(index)).containsExactly(json);
	}
}
