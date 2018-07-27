package im.ligas.kafka.stream;

import java.util.Map;

public class Processor {
    private String name;
    private Map<String, String> props;


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, String> getProps() {
        return props;
    }

    public void setProps(Map<String, String> prop) {
        this.props = prop;
    }
}
