package storm.starter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.ComponentObject;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.NullStruct;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StateSpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BasicBoltExecutor;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IComponent;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.IRichStateSpout;
import backtype.storm.topology.OutputFieldsGetter;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import com.google.common.base.Preconditions;

/**
 * TopologyBuilder exposes the Java API for specifying a topology for Storm to
 * execute. Topologies are Thrift structures in the end, but since the Thrift
 * API is so verbose, TopologyBuilder greatly eases the process of creating
 * topologies.
 * 
 * <p>
 * The pattern for TopologyBuilder is to map component ids to components using
 * the setSpout and setBolt methods. Those methods return objects that are then
 * used to declare the inputs for that component.
 * </p>
 */
public class TopologyBuilder {
	private int nextId = 1;
	private Map<Integer, IRichBolt> _bolts = new HashMap<Integer, IRichBolt>();
	private Map<Integer, Map<GlobalStreamId, Grouping>> _inputs = new HashMap<Integer, Map<GlobalStreamId, Grouping>>();
	private Map<Integer, SpoutSpec> _spouts = new HashMap<Integer, SpoutSpec>();
	private Map<Integer, StateSpoutSpec> _stateSpouts = new HashMap<Integer, StateSpoutSpec>();
	private Map<Integer, Integer> _boltParallelismHints = new HashMap<Integer, Integer>();
	
	private Map<Integer, IComponent> _refs = new HashMap<Integer, IComponent>();

	public StormTopology createTopology() {
		Map<Integer, Bolt> boltSpecs = new HashMap<Integer, Bolt>();
		for (Integer boltId : _bolts.keySet()) {
			IRichBolt bolt = _bolts.get(boltId);
			Integer parallelism_hint = _boltParallelismHints.get(boltId);
			Map<GlobalStreamId, Grouping> inputs = _inputs.get(boltId);
			ComponentCommon common = getComponentCommon(bolt, parallelism_hint);
			if (parallelism_hint != null) {
				common.set_parallelism_hint(parallelism_hint);
			}
			boltSpecs.put(boltId, new Bolt(inputs, ComponentObject.serialized_java(Utils.serialize(bolt)), common));
		}
		return new StormTopology(new HashMap<Integer, SpoutSpec>(_spouts), boltSpecs,
				new HashMap<Integer, StateSpoutSpec>(_stateSpouts));
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
		final int id = nextId;
		nextId++;
		validateUnusedId(id);
		_refs.put(id, bolt);
		_bolts.put(id, bolt);
		_boltParallelismHints.put(id, parallelism_hint);
		_inputs.put(id, new HashMap<GlobalStreamId, Grouping>());
		return new InputGetter(id);
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
		return setBolt(new BasicBoltExecutor(bolt), parallelism_hint);
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
		final int id = nextId;
		nextId++;
		validateUnusedId(id);
		_refs.put(id, spout);
		_spouts.put(
				id,
				new SpoutSpec(ComponentObject.serialized_java(Utils.serialize(spout)), getComponentCommon(spout,
						parallelism_hint), spout.isDistributed()));
	}

	public void setStateSpout(IRichStateSpout stateSpout) {
		setStateSpout(stateSpout, null);
	}

	public void setStateSpout(IRichStateSpout stateSpout, Integer parallelism_hint) {
		final int id = nextId;
		nextId++;
		validateUnusedId(id);
		_refs.put(id, stateSpout);
		_stateSpouts.put(id, new StateSpoutSpec(ComponentObject.serialized_java(Utils.serialize(stateSpout)),
				getComponentCommon(stateSpout, parallelism_hint)));
	}

	private void validateUnusedId(int id) {
		if (_bolts.containsKey(id)) {
			throw new IllegalArgumentException("Bolt has already been declared for id " + id);
		}
		if (_spouts.containsKey(id)) {
			throw new IllegalArgumentException("Spout has already been declared for id " + id);
		}
		if (_stateSpouts.containsKey(id)) {
			throw new IllegalArgumentException("State spout has already been declared for id " + id);
		}
	}

	private ComponentCommon getComponentCommon(IComponent component, Integer parallelism_hint) {
		OutputFieldsGetter getter = new OutputFieldsGetter();
		component.declareOutputFields(getter);
		ComponentCommon common = new ComponentCommon(getter.getFieldsDeclaration());
		if (parallelism_hint != null) {
			common.set_parallelism_hint(parallelism_hint);
		}
		return common;

	}

	protected class InputGetter implements InputDeclarer {
		private int _boltId;

		public InputGetter(int boltId) {
			_boltId = boltId;
		}

		@Override
		public InputDeclarer fieldsGrouping(IComponent component, String... fields) {
			return fieldsGrouping(component, new Fields(fields));
		}

		@Override
		public InputDeclarer fieldsGrouping(IComponent component, int streamId, String... fields) {
			return fieldsGrouping(component, streamId, new Fields(fields));
		}

		public InputDeclarer fieldsGrouping(IComponent component, Fields fields) {
			return fieldsGrouping(component, Utils.DEFAULT_STREAM_ID, fields);
		}

		public InputDeclarer fieldsGrouping(IComponent component, int streamId, Fields fields) {
			return grouping(component, streamId, Grouping.fields(fields.toList()));
		}

		public InputDeclarer globalGrouping(IComponent component) {
			return globalGrouping(component, Utils.DEFAULT_STREAM_ID);
		}

		public InputDeclarer globalGrouping(IComponent component, int streamId) {
			return grouping(component, streamId, Grouping.fields(new ArrayList<String>()));
		}

		public InputDeclarer shuffleGrouping(IComponent component) {
			return shuffleGrouping(component, Utils.DEFAULT_STREAM_ID);
		}

		public InputDeclarer shuffleGrouping(IComponent component, int streamId) {
			return grouping(component, streamId, Grouping.shuffle(new NullStruct()));
		}

		public InputDeclarer noneGrouping(IComponent component) {
			return noneGrouping(component, Utils.DEFAULT_STREAM_ID);
		}

		public InputDeclarer noneGrouping(IComponent component, int streamId) {
			return grouping(component, streamId, Grouping.none(new NullStruct()));
		}

		public InputDeclarer allGrouping(IComponent component) {
			return allGrouping(component, Utils.DEFAULT_STREAM_ID);
		}

		public InputDeclarer allGrouping(IComponent component, int streamId) {
			return grouping(component, streamId, Grouping.all(new NullStruct()));
		}

		public InputDeclarer directGrouping(IComponent component) {
			return directGrouping(component, Utils.DEFAULT_STREAM_ID);
		}

		public InputDeclarer directGrouping(IComponent component, int streamId) {
			return grouping(component, streamId, Grouping.direct(new NullStruct()));
		}

		private InputDeclarer grouping(IComponent component, int streamId, Grouping grouping) {
			_inputs.get(_boltId).put(new GlobalStreamId(getComponentId(component), streamId), grouping);
			return this;
		}

		private int getComponentId(IComponent component) {
			return Preconditions.checkNotNull(getKeyByValue(_refs, component),
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
