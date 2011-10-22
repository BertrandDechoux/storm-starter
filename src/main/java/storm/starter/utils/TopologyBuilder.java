package storm.starter.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IComponent;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.IRichStateSpout;
import backtype.storm.tuple.Fields;

import com.google.common.base.Preconditions;

/**
 * A wrapper around {@link backtype.storm.topology.TopologyBuilder} that manages
 * id by itself. </p>
 */
public class TopologyBuilder {
	private final backtype.storm.topology.TopologyBuilder topologyBuilder = new backtype.storm.topology.TopologyBuilder();
	private final Map<Integer, IComponent> references = new HashMap<Integer, IComponent>();
	private int nextId = 1;

	public StormTopology createTopology() {
		return topologyBuilder.createTopology();
	}

	/**
	 * Define a new bolt in this topology with parallelism of just one thread.
	 * 
	 * @param bolt
	 *            the bolt
	 * @return use the returned object to declare the inputs to this component
	 */
	public InputDeclarer setBolt(IRichBolt bolt) {
		return setBolt(bolt, null);
	}

	/**
	 * Define a new bolt in this topology with the specified amount of
	 * parallelism.
	 * 
	 * @param bolt
	 *            the bolt
	 * @param parallelism_hint
	 *            the number of tasks that should be assigned to execute this
	 *            bolt. Each task will run on a thread in a process somewhere
	 *            around the cluster.
	 * @return use the returned object to declare the inputs to this component
	 */
	public InputDeclarer setBolt(IRichBolt bolt, Integer parallelism_hint) {
		return new InputGetter(topologyBuilder.setBolt(getNextIdFor(bolt),
				bolt, parallelism_hint));
	}

	/**
	 * Define a new bolt in this topology. This defines a basic bolt, which is a
	 * simpler to use but more restricted kind of bolt. Basic bolts are intended
	 * for non-aggregation processing and automate the anchoring/acking process
	 * to achieve proper reliability in the topology.
	 * 
	 * @param bolt
	 *            the basic bolt
	 * @return use the returned object to declare the inputs to this component
	 */
	public InputDeclarer setBolt(IBasicBolt bolt) {
		return setBolt(bolt, null);
	}

	/**
	 * Define a new bolt in this topology. This defines a basic bolt, which is a
	 * simpler to use but more restricted kind of bolt. Basic bolts are intended
	 * for non-aggregation processing and automate the anchoring/acking process
	 * to achieve proper reliability in the topology.
	 * 
	 * @param bolt
	 *            the basic bolt
	 * @param parallelism_hint
	 *            the number of tasks that should be assigned to execute this
	 *            bolt. Each task will run on a thread in a process somewehere
	 *            around the cluster.
	 * @return use the returned object to declare the inputs to this component
	 */
	public InputDeclarer setBolt(IBasicBolt bolt, Integer parallelism_hint) {
		return new InputGetter(topologyBuilder.setBolt(getNextIdFor(bolt),
				bolt, parallelism_hint));
	}

	/**
	 * Define a new spout in this topology.
	 * 
	 * @param spout
	 *            the spout
	 */
	public void setSpout(IRichSpout spout) {
		setSpout(spout, null);
	}

	/**
	 * Define a new spout in this topology with the specified parallelism. If
	 * the spout declares itself as non-distributed, the parallelism_hint will
	 * be ignored and only one task will be allocated to this component.
	 * 
	 * @param parallelism_hint
	 *            the number of tasks that should be assigned to execute this
	 *            spout. Each task will run on a thread in a process somewhere
	 *            around the cluster.
	 * @param spout
	 *            the spout
	 */
	public void setSpout(IRichSpout spout, Integer parallelism_hint) {
		topologyBuilder.setSpout(getNextIdFor(spout), spout);
	}

	public void setStateSpout(IRichStateSpout stateSpout) {
		setStateSpout(stateSpout, null);
	}

	public void setStateSpout(IRichStateSpout stateSpout,
			Integer parallelism_hint) {
		topologyBuilder.setStateSpout(getNextIdFor(stateSpout), stateSpout,
				parallelism_hint);
	}

	private int getNextIdFor(final IComponent component) {
		final int id = nextId;
		nextId++;
		references.put(id, component);
		return id;
	}

	protected class InputGetter implements InputDeclarer {
		private final backtype.storm.topology.InputDeclarer inputDeclarer;

		public InputGetter(
				final backtype.storm.topology.InputDeclarer inputDeclarer) {
			this.inputDeclarer = inputDeclarer;
		}

		@Override
		public InputDeclarer fieldsGrouping(IComponent component,
				String... fields) {
			inputDeclarer.fieldsGrouping(getComponentId(component), new Fields(
					fields));
			return this;
		}

		@Override
		public InputDeclarer fieldsGrouping(IComponent component, int streamId,
				String... fields) {
			inputDeclarer.fieldsGrouping(getComponentId(component), streamId,
					new Fields(fields));
			return this;
		}

		public InputDeclarer fieldsGrouping(IComponent component, Fields fields) {
			inputDeclarer.fieldsGrouping(getComponentId(component), fields);
			return this;
		}

		public InputDeclarer fieldsGrouping(IComponent component, int streamId,
				Fields fields) {
			inputDeclarer.fieldsGrouping(getComponentId(component), streamId,
					fields);
			return this;
		}

		public InputDeclarer globalGrouping(IComponent component) {
			inputDeclarer.globalGrouping(getComponentId(component));
			return this;
		}

		public InputDeclarer globalGrouping(IComponent component, int streamId) {
			inputDeclarer.globalGrouping(getComponentId(component), streamId);
			return this;
		}

		public InputDeclarer shuffleGrouping(IComponent component) {
			inputDeclarer.shuffleGrouping(getComponentId(component));
			return this;
		}

		public InputDeclarer shuffleGrouping(IComponent component, int streamId) {
			inputDeclarer.shuffleGrouping(getComponentId(component), streamId);
			return this;
		}

		public InputDeclarer noneGrouping(IComponent component) {
			inputDeclarer.noneGrouping(getComponentId(component));
			return this;
		}

		public InputDeclarer noneGrouping(IComponent component, int streamId) {
			inputDeclarer.noneGrouping(getComponentId(component), streamId);
			return this;
		}

		public InputDeclarer allGrouping(IComponent component) {
			inputDeclarer.allGrouping(getComponentId(component));
			return this;
		}

		public InputDeclarer allGrouping(IComponent component, int streamId) {
			inputDeclarer.allGrouping(getComponentId(component), streamId);
			return this;
		}

		public InputDeclarer directGrouping(IComponent component) {
			inputDeclarer.directGrouping(getComponentId(component));
			return this;
		}

		public InputDeclarer directGrouping(IComponent component, int streamId) {
			inputDeclarer.directGrouping(getComponentId(component), streamId);
			return this;
		}

		private int getComponentId(IComponent component) {
			return Preconditions.checkNotNull(
					getKeyByValue(references, component),
					"This component has not been yet registered " + component);
		}

		private <K> K getKeyByValue(Map<K, ?> map, Object value) {
			for (Entry<K, ?> entry : map.entrySet()) {
				if (value.equals(entry.getValue())) {
					return entry.getKey();
				}
			}
			return null;
		}

	}
}
