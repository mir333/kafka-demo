package im.ligas.kafka.stream;

import java.util.Map;

public class TargetSystem {
    private String targetSystem;
    private Map<String, String> props;


    public TargetSystem() {
        this.targetSystem= null;
        this.props = null;
    }

    public TargetSystem(String targetSystem, Map<String, String> props) {
        this.targetSystem = targetSystem;
        this.props = props;
    }

    public String getTargetSystem() {
        return targetSystem;
    }

    public void setTargetSystem(String targetSystem) {
        this.targetSystem = targetSystem;
    }

    public Map<String, String> getProps() {
        return props;
    }

    public void setProps(Map<String, String> props) {
        this.props = props;
    }
}
