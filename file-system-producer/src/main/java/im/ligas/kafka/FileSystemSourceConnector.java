package im.ligas.kafka;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static im.ligas.kafka.FileSystemSourceConnectorConfig.*;

public class FileSystemSourceConnector extends SourceConnector {
    private static final Logger log = LoggerFactory.getLogger(FileSystemSourceConnector.class);
    private FileSystemSourceConnectorConfig config;

    private String folderName;
    private String topic;
    private int batchSize;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        config = new FileSystemSourceConnectorConfig(map);

        //TODO: Support for folders
        folderName = config.getFodlerName();
        if (StringUtils.isNotBlank(folderName)) {
            throw new ConfigException("Folder name was not set");
        }
        if (!new File(folderName).isDirectory()) {
            throw new ConfigException("Folder name does nto refer to a real folder.");
        }
        List<String> topics = config.getTopics();
        if (topics.size() != 1) {
            throw new ConfigException("'topic' in FileSystemSourceConnector configuration requires definition of a single topic");
        }
        topic = topics.get(0);
        batchSize = config.getBatchSize();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return FileSystemSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();
        //TODO: Add code to handle multiple processing
        Map<String, String> config = new HashMap<>();
        if (folderName != null) {
            config.put(FOLDER_NAME_CONFIG, folderName);
        }
        config.put(TOPIC_CONFIG, topic);
        config.put(TASK_BATCH_SIZE_CONFIG, String.valueOf(batchSize));
        configs.add(config);

        return configs;
    }

    @Override
    public void stop() {
        //nothing
    }

    @Override
    public ConfigDef config() {
        return FileSystemSourceConnectorConfig.conf();
    }
}
