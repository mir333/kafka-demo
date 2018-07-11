package im.ligas.kafka;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.List;
import java.util.Map;


public class FileSystemSinkConnectorConfig extends AbstractConfig {

    public static final String FOLDER_NAME_CONFIG = "folder.name";
    private static final String FOLDER_NAME_SETTING_DOC = "The folder to be processed";
    public static final String TOPICS_CONFIG = "topics";
    private static final String TOPICS_SETTING_DOC = "The topic to publish data to";

    public FileSystemSinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public FileSystemSinkConnectorConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        return new ConfigDef()
                .define(FOLDER_NAME_CONFIG, Type.STRING, Importance.HIGH, FOLDER_NAME_SETTING_DOC)
                .define(TOPICS_CONFIG, Type.LIST, Importance.HIGH, TOPICS_SETTING_DOC);
    }

    public String getFolderName() {
        return this.getString(FOLDER_NAME_CONFIG);
    }

    public List<String> getTopics() {
        return this.getList(TOPICS_CONFIG);
    }
}
