package storm.starter;

import backtype.storm.topology.IComponent;
import backtype.storm.tuple.Fields;

public interface InputDeclarer {
    public InputDeclarer fieldsGrouping(IComponent component, String... fields);
    public InputDeclarer fieldsGrouping(IComponent component, int streamId, String... fields);
    
    public InputDeclarer fieldsGrouping(IComponent component, Fields fields);
    public InputDeclarer fieldsGrouping(IComponent component, int streamId, Fields fields);

    public InputDeclarer globalGrouping(IComponent component);
    public InputDeclarer globalGrouping(IComponent component, int streamId);

    public InputDeclarer shuffleGrouping(IComponent component);
    public InputDeclarer shuffleGrouping(IComponent component, int streamId);

    public InputDeclarer noneGrouping(IComponent component);
    public InputDeclarer noneGrouping(IComponent component, int streamId);

    public InputDeclarer allGrouping(IComponent component);
    public InputDeclarer allGrouping(IComponent component, int streamId);

    public InputDeclarer directGrouping(IComponent component);
    public InputDeclarer directGrouping(IComponent component, int streamId);
}
