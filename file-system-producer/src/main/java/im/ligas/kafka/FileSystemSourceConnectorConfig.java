package im.ligas.kafka;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.List;
import java.util.Map;


public class FileSystemSourceConnectorConfig extends AbstractConfig {

    public static final String FOLDER_NAME_CONFIG = "folder.name";
    private static final String FOLDER_NAME_SETTING_DOC = "The folder to be processed";
    public static final String TOPIC_CONFIG = "topic";
    private static final String TOPIC_SETTING_DOC = "The topic to publish data to";
    public static final String TASK_BATCH_SIZE_CONFIG = "batch.size";
    private static final String TASK_BATCH_SIZE_SETTING_DOC = "The maximum number of records the Source task can read from file one time";

    public static final int DEFAULT_TASK_BATCH_SIZE = 2000;

    public FileSystemSourceConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public FileSystemSourceConnectorConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        return new ConfigDef()
                .define(FOLDER_NAME_CONFIG, Type.STRING, Importance.HIGH, FOLDER_NAME_SETTING_DOC)
                .define(TOPIC_CONFIG, Type.LIST, Importance.HIGH, TOPIC_SETTING_DOC)
                .define(TASK_BATCH_SIZE_CONFIG, Type.INT, DEFAULT_TASK_BATCH_SIZE, Importance.LOW, TASK_BATCH_SIZE_SETTING_DOC);
    }

    public String getFodlerName() {
        return this.getString(FOLDER_NAME_CONFIG);
    }

    public List<String> getTopics() {
        return this.getList(TOPIC_CONFIG);
    }

    public int getBatchSize() {
        return this.getInt(TASK_BATCH_SIZE_CONFIG);
    }
}
