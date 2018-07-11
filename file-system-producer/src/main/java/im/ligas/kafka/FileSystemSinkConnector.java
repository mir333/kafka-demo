package im.ligas.kafka;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static im.ligas.kafka.FileSystemSinkConnectorConfig.FOLDER_NAME_CONFIG;

public class FileSystemSinkConnector extends SinkConnector {
    private static Logger log = LoggerFactory.getLogger(FileSystemSinkConnector.class);

    private FileSystemSinkConnectorConfig config;
    private String folderName;
    private String topic;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        log.error(map.keySet().toString());

        config = new FileSystemSinkConnectorConfig(map);
        //TODO: Support for folders
        folderName = config.getFolderName();
        if (StringUtils.isBlank(folderName)) {
            throw new ConfigException("Folder name was not set");
        }
        List<String> topics = config.getTopics();
        if (topics.size() != 1) {
            throw new ConfigException("'topic' in FileSystemSourceConnector configuration requires definition of a single topic");
        }
        topic = topics.get(0);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return FileSystemSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> config = new HashMap<>();
            if (folderName != null) {
                config.put(FOLDER_NAME_CONFIG, folderName);
            }
            config.put(FileSystemSinkConnectorConfig.TOPICS_CONFIG, topic);
            configs.add(config);
        }
        return configs;
    }

    @Override
    public void stop() {
        //nothing
    }

    @Override
    public ConfigDef config() {
        return FileSystemSinkConnectorConfig.conf();
    }
}
